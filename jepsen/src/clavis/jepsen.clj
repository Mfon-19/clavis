(ns clavis.jepsen
    "Jepsen release-candidate tests for Clavis.

    The workload checks the real product contract rather than the raw mutex RPC:
    a worker acquires a lock, receives a fencing token, and writes to a downstream
    register only if the token is greater than the register's current token."
    (:gen-class)
    (:require
      [cheshire.core :as json]
      [clojure.edn :as edn]
      [clojure.java.io :as io]
      [clojure.java.shell :as shell]
      [clojure.string :as str]
      [clojure.tools.logging :refer [info warn]]
      [jepsen.checker :as checker]
      [jepsen.cli :as cli]
      [jepsen.client :as client]
      [jepsen.control :as c]
      [jepsen.db :as db]
      [jepsen.generator :as gen]
      [jepsen.nemesis :as nemesis]
      [jepsen.os.debian :as debian]
      [jepsen.tests :as tests])
    (:import
      (java.io RandomAccessFile)
      (java.util UUID)))

(def remote-binary "/opt/clavis/clavis")
(def data-root "/var/lib/clavis")
(def log-file "/var/log/clavis.log")
(def pid-file "/var/run/clavis.pid")
(def ^:dynamic *register-path* "/tmp/clavis-jepsen-registers.edn")
(def ^:dynamic *register-lock-path* "/tmp/clavis-jepsen-registers.lock")

(def node-ids
  ["11111111-1111-1111-1111-111111111111"
   "22222222-2222-2222-2222-222222222222"
   "33333333-3333-3333-3333-333333333333"
   "44444444-4444-4444-4444-444444444444"
   "55555555-5555-5555-5555-555555555555"
   "66666666-6666-6666-6666-666666666666"
   "77777777-7777-7777-7777-777777777777"])

(defn node-id
  [test node]
  (let [idx (.indexOf ^java.util.List (:nodes test) node)]
    (if (neg? idx)
      (throw (ex-info "node is not in test node list" {:node node :nodes (:nodes test)}))
      (or (nth node-ids idx nil)
          (throw (ex-info "add more deterministic node IDs" {:node node :index idx}))))))

(defn sh
  [& parts]
  (c/exec :bash :-lc (str/join " " parts)))

(defn sh!
  [& commands]
  (c/exec :bash :-lc (str/join " ; " commands)))

(defn kill-clavis!
  []
  (sh! (str "if test -f " pid-file
            "; then kill $(cat " pid-file ") 2>/dev/null || true; fi")
       "pkill -9 -x clavis 2>/dev/null || true"
       (str "rm -f " pid-file)))

(defn seed-targets
  [test node]
  (->> (:nodes test)
       (remove #{node})
       (map #(str % ":9000"))))

(defn select-join-target!
  [test node]
  (let [targets (seed-targets test node)
        probe-script (str
                      "for i in $(seq 1 60); do "
                      "for target in " (str/join " " targets) "; do "
                      "host=${target%:*}; port=${target#*:}; "
                      "(: </dev/tcp/$host/$port) >/dev/null 2>&1 && echo $target && exit 0; "
                      "done; "
                      "sleep 1; "
                      "done; "
                      "exit 1")
        join-target (str/trim (c/exec :bash :-lc probe-script))]
    (when (str/blank? join-target)
      (throw (ex-info "failed to find a reachable join target" {:node node :targets targets})))
    join-target))

(defn start-clavis!
  [test node]
  (let [bootstrap? (= node (first (:nodes test)))
        data-dir (str data-root "/" (node-id test node))
        base [remote-binary
              "--node-id" (node-id test node)
              "--raft-addr" ":7000"
              "--raft-advertise-addr" (str node ":7000")
              "--grpc-addr" ":9000"
              "--grpc-advertise-addr" (str node ":9000")
              "--data-dir" data-dir]]
    (sh "mkdir -p" data-dir)
    (let [args (if bootstrap?
                 (conj base "--bootstrap")
                 (conj base "--join" (select-join-target! test node)))]
      (info "starting clavis" node (str/join " " args))
      (c/exec :bash :-lc
              (str "nohup " (str/join " " args)
                   " >> " log-file " 2>&1 & echo $! > " pid-file)))
    (Thread/sleep (if bootstrap? 3000 5000))))

(defrecord ClavisDB [binary]
  db/DB
  (setup! [_ test node]
    (info "installing clavis on" node)
    (sh "mkdir -p /opt/clavis")
    (c/upload binary remote-binary)
    (sh! (str "chmod +x " remote-binary)
         (str "mkdir -p " data-root)
         (str "rm -rf " data-root "/*")
         (str "touch " log-file))
    (kill-clavis!)
    (start-clavis! test node))

  (teardown! [_ _ node]
    (info "stopping clavis on" node)
    (kill-clavis!))

  db/LogFiles
  (log-files [_ _ _]
    [log-file]))

(defn json-body
  [body]
  (when (and body (seq body))
        (try
          (json/parse-string body true)
          (catch Exception _
            nil))))

(defn grpc-call
  [test target service method body]
  (let [payload (json/generate-string (or body {}))
        result (apply shell/sh
                      (remove nil?
                              [(:grpcurl test)
                               "-plaintext"
                               "-import-path" (:proto-import-path test)
                               "-proto" (:proto test)
                               "-d" payload
                               target
                               (str service "/" method)]))]
    {:exit (:exit result)
     :status (if (zero? (:exit result)) 200 500)
     :body (:out result)
     :error (:err result)
     :target target
     :json (json-body (:out result))}))

(defn success?
  [resp]
  (zero? (:exit resp)))

(defn busy?
  [resp]
  (let [err (:error resp "")]
    (or (str/includes? err "FailedPrecondition")
        (str/includes? err "lock is already held"))))

(defn field
  [m & ks]
  (some (fn [k]
          (or (get m k)
              (get m (keyword (name k)))
              (get m (keyword (str/replace (name k) #"_" "")))
              (get m (keyword (str/replace (name k) #"_([a-z])"
                                           #(str/upper-case (second %)))))
              (get m (str/replace (name k) #"_([a-z])"
                                  #(str/upper-case (second %))))
              (get m (name k))))
        ks))

(defn long-value
  [x]
  (cond
   (nil? x) nil
   (integer? x) (long x)
   (number? x) (long x)
   (string? x) (Long/parseUnsignedLong x)
   :else (throw (ex-info "cannot coerce value to long" {:value x :type (type x)}))))

(defn grpc-targets
  [test preferred-node]
  (map #(str % ":9000")
       (distinct (remove nil? (cons preferred-node (:nodes test))))))

(defn try-lock-rpc!
  [test preferred-node method body]
  (let [attempts (for [target (grpc-targets test preferred-node)]
                   (grpc-call test target "clavis.v1.LockService" method body))]
    (or (some #(when (success? %) %) attempts)
        (some #(when (busy? %) %) attempts)
        (last attempts))))

(defn create-lease!
  [test node ttl-seconds owner-id]
  (let [resp (try-lock-rpc! test node "CreateLease"
                            {:ownerId owner-id
                             :ttlSeconds ttl-seconds})
        lease-id (long-value (field (:json resp) :leaseId :lease_id))]
    (if (and (success? resp) lease-id)
      lease-id
      (throw (ex-info "create lease failed" {:response resp})))))

(defn acquire!
  [test node lock-name lease-id owner-id]
  (let [resp (try-lock-rpc! test node "AcquireLock"
                            {:lockName lock-name
                             :ownerId owner-id
                             :leaseId lease-id})]
    (if (success? resp)
      {:ok? true
       :token (long-value (field (:json resp) :fencingToken :fencing_token))
       :response resp}
      {:ok? false
       :response resp})))

(defn release!
  [test node lock-name lease-id]
  (try-lock-rpc! test node "ReleaseLock"
                 {:lockName lock-name
                  :leaseId lease-id}))

(defn read-registers
  []
  (let [file (io/file *register-path*)]
    (if (.exists file)
      (edn/read-string (slurp file))
      {})))

(defn write-registers!
  [registers]
  (spit *register-path* (pr-str registers)))

(defn reset-registers!
  []
  (write-registers! {}))

(defn accept-write!
  [resource token value]
  (with-open [raf (RandomAccessFile. ^String *register-lock-path* "rw")
              channel (.getChannel raf)
              lock (.lock channel)]
    (let [registers (read-registers)
          current-token (get-in registers [resource :token] 0)]
      (if (< current-token token)
        (do
          (write-registers! (assoc registers resource {:token token
                                                       :value value}))
          true)
        false))))

(defrecord ClavisClient [node]
  client/Client
  (open! [this _ node]
    (assoc this :node node))

  (setup! [this _]
    this)

  (invoke! [this test op]
    (let [{:keys [resource value pause?]} (:value op)
          owner-id (str "jepsen-" node "-" (:process op) "-" (UUID/randomUUID))
          ttl-seconds (:lease-ttl test)
          lock-name (str "resource:" resource)]
      (try
        (let [lease-id (create-lease! test node ttl-seconds owner-id)
              acquired (acquire! test node lock-name lease-id owner-id)]
          (if-not (:ok? acquired)
                  (assoc op :type :fail :value (assoc (:value op)
                                                      :error :busy
                                                      :status (get-in acquired [:response :status])))
                  (let [token (:token acquired)
                        pause-ms (if pause?
                                   (max (:stale-hold-ms test)
                                        (* 1000 (+ ttl-seconds 2)))
                                   (:hold-ms test))]
                    (when (pos? pause-ms)
                          (Thread/sleep (long pause-ms)))
                    (let [accepted? (accept-write! resource token value)]
                      (try
                        (release! test node lock-name lease-id)
                        (catch Exception e
                          (warn e "release failed")))
                      (if accepted?
                        (assoc op :type :ok :value (assoc (:value op)
                                                          :token token
                                                          :lease-id lease-id))
                        (assoc op :type :fail :value (assoc (:value op)
                                                            :token token
                                                            :lease-id lease-id
                                                            :error :stale-token)))))))
        (catch Exception e
          (assoc op :type :info :error (.getMessage e))))))

  (teardown! [this _]
    this)

  (close! [_ _]
    nil))

(defrecord FencedRegisterChecker []
  checker/Checker
  (check [_ _ history _]
    (let [accepted (loop [pending {}
                          accepted []
                          ops history]
                     (if-let [op (first ops)]
                       (let [op-key [(:process op) (:f op)]]
                         (cond
                          (= :invoke (:type op))
                          (recur (assoc pending op-key op) accepted (rest ops))

                          (and (= :ok (:type op))
                               (= :fenced-write (:f op)))
                          (let [started (get pending op-key)]
                            (recur (dissoc pending op-key)
                              (conj accepted
                                    (assoc (:value op)
                                           :invoke-time (:time started)
                                           :complete-time (:time op)
                                           :complete-index (:index op)))
                              (rest ops)))

                          :else
                          (recur (dissoc pending op-key) accepted (rest ops))))
                       accepted))
          grouped (group-by :resource accepted)
          violations (->> grouped
                          (mapcat
                           (fn [[resource ops]]
                             (for [a ops
                                   b ops
                                   :when (and (not= (:complete-index a) (:complete-index b))
                                              (not (< (:token a) (:token b)))
                                              (< (:complete-time a) (:invoke-time b)))]
                               {:resource resource
                                :previous a
                                :current b}))))
          violations (vec violations)]
      {:valid? (empty? violations)
       :accepted-count (count accepted)
       :resource-count (count grouped)
       :violations violations})))

(defn op
  [test]
  (let [resource (str "resource-" (rand-int (:resources test)))
        pause? (< (rand) (:stale-probability test))]
    {:type :invoke
     :f :fenced-write
     :value {:resource resource
             :value (str (UUID/randomUUID))
             :pause? pause?}}))

(defn partition-halves
  [nodes]
  (let [shuffled (shuffle nodes)
        n (max 1 (quot (count shuffled) 2))]
    [(take n shuffled) (drop n shuffled)]))

(defn reset-partition!
  [test]
  (doseq [node (:nodes test)]
    (c/on node
          (sh "iptables -F CLAVIS_JEPSEN 2>/dev/null || true"))))

(defn start-partition!
  [test]
  (let [[a b] (partition-halves (:nodes test))]
    (info "partitioning" a b)
    (doseq [node (:nodes test)]
      (let [blocked (if (some #{node} a) b a)]
        (c/on node
              (sh! "iptables -N CLAVIS_JEPSEN 2>/dev/null || true"
                   "iptables -C INPUT -j CLAVIS_JEPSEN 2>/dev/null || iptables -I INPUT -j CLAVIS_JEPSEN"
                   "iptables -C OUTPUT -j CLAVIS_JEPSEN 2>/dev/null || iptables -I OUTPUT -j CLAVIS_JEPSEN"
                   "iptables -F CLAVIS_JEPSEN")
              (doseq [peer blocked]
                (sh! (str "iptables -A CLAVIS_JEPSEN -s " peer " -j DROP")
                     (str "iptables -A CLAVIS_JEPSEN -d " peer " -j DROP"))))))))

(defn restart-node!
  [test]
  (let [node (rand-nth (:nodes test))]
    (info "restarting clavis on" node)
    (c/on node
          (kill-clavis!)
          (start-clavis! test node))))

(defn pause-node!
  [_ test]
  (let [node (rand-nth (:nodes test))]
    (info "pausing clavis on" node)
    (c/on node
          (sh "if test -f" pid-file "; then kill -STOP $(cat" pid-file ") 2>/dev/null || true; fi"))
    node))

(defn resume-node!
  [node]
  (when node
        (info "resuming clavis on" node)
        (c/on node
              (sh "if test -f" pid-file "; then kill -CONT $(cat" pid-file ") 2>/dev/null || true; fi"))))

(defn skew-clock!
  [test seconds]
  (let [node (rand-nth (:nodes test))
        offset (if (zero? (rand-int 2)) seconds (- seconds))]
    (info "skewing clock on" node "by" offset "seconds")
    (c/on node
          (sh (str "date -s '" offset " seconds' >/dev/null 2>&1 || true")))
    node))

(defn reset-clock!
  [test]
  (doseq [node (:nodes test)]
    (c/on node
          (sh "chronyc makestep >/dev/null 2>&1 ||"
              "systemctl restart systemd-timesyncd >/dev/null 2>&1 ||"
              "service ntp restart >/dev/null 2>&1 || true"))))

(defrecord ClavisNemesis [paused-node]
  nemesis/Nemesis
  (setup! [this _]
    this)

  (invoke! [this test op]
    (try
      (case (:f op)
            :partition-start
            (do
              (start-partition! test)
              (assoc op :type :info :value :partitioned))

            :partition-stop
            (do
              (reset-partition! test)
              (assoc op :type :info :value :healed))

            :restart
            (do
              (restart-node! test)
              (assoc op :type :info :value :restarted))

            :pause
            (let [node (pause-node! this test)]
              (reset! paused-node node)
              (assoc op :type :info :value {:paused node}))

            :resume
            (do
              (resume-node! @paused-node)
              (reset! paused-node nil)
              (assoc op :type :info :value :resumed))

            :clock-skew
            (let [node (skew-clock! test (:clock-skew-seconds test))]
              (assoc op :type :info :value {:skewed node}))

            :clock-reset
            (do
              (reset-clock! test)
              (assoc op :type :info :value :clock-reset))

            (assoc op :type :info :value :noop))
      (catch Exception e
        (assoc op :type :info :error (.getMessage e)))))

  (teardown! [this test]
    (reset-partition! test)
    (resume-node! @paused-node)
    (reset! paused-node nil)
    (reset-clock! test)
    this))

(defn nemesis-op-cycle
  []
  (cycle [(gen/sleep 10)
          {:type :info :f :partition-start}
          (gen/sleep 10)
          {:type :info :f :partition-stop}
          (gen/sleep 5)
          {:type :info :f :pause}
          (gen/sleep 20)
          {:type :info :f :resume}
          (gen/sleep 5)
          {:type :info :f :restart}
          (gen/sleep 10)
          {:type :info :f :clock-skew}
          (gen/sleep 10)
          {:type :info :f :clock-reset}]))

(defn workload
  [test]
  (gen/phases
   (->> (repeatedly #(op test))
        (gen/stagger (/ 1 (:rate test)))
        (gen/nemesis (nemesis-op-cycle))
        (gen/time-limit (:time-limit test)))
   (gen/log "healing nemeses")
   (gen/nemesis (gen/once {:type :info :f :partition-stop}))
   (gen/nemesis (gen/once {:type :info :f :resume}))
   (gen/nemesis (gen/once {:type :info :f :clock-reset}))
   (gen/sleep 10)))

(def cli-opts
  [[nil "--binary PATH" "Path to the local clavis binary."
    :default "../clavis"]
   [nil "--grpcurl PATH" "Path to the local grpcurl executable."
    :default "grpcurl"]
   [nil "--proto PATH" "Path to the Clavis API proto file."
    :default "../api/proto/lock.proto"]
   [nil "--proto-import-path PATH" "Import path for Clavis API protos."
    :default "../api/proto"]
   [nil "--lease-ttl SECONDS" "Clavis lease TTL used by Jepsen operations."
    :default 15
    :parse-fn parse-long]
   [nil "--resources N" "Number of logical resources to coordinate."
    :default 3
    :parse-fn parse-long]
   [nil "--rate HZ" "Approximate client operation rate."
    :default 5.0
    :parse-fn #(Double/parseDouble %)]
   [nil "--hold-ms MS" "Normal post-acquire hold time before a fenced write."
    :default 0
    :parse-fn parse-long]
   [nil "--stale-hold-ms MS" "Paused-holder sleep before attempting a stale write."
    :default 20000
    :parse-fn parse-long]
   [nil "--stale-probability P" "Probability that an operation simulates a paused holder."
    :default 0.05
    :parse-fn #(Double/parseDouble %)]
   [nil "--clock-skew-seconds SECONDS" "Clock skew magnitude applied by the clock nemesis."
    :default 30
    :parse-fn parse-long]])

(defn clavis-test
  [opts]
  (reset-registers!)
  (merge tests/noop-test
         opts
         {:name "clavis-fenced-register"
          :os debian/os
          :db (map->ClavisDB {:binary (:binary opts)})
          :client (map->ClavisClient {})
          :nemesis (map->ClavisNemesis {:paused-node (atom nil)})
          :checker (FencedRegisterChecker.)
          :generator (workload opts)
          :register-path *register-path*}))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn clavis-test
                                  :usage (cli/test-usage)
                                  :opt-spec cli-opts})
            args))
