(ns clavis.jepsen
    "Jepsen release-candidate tests for Clavis.

    The workload checks the real product contract rather than the raw mutex RPC:
    a worker acquires a lock, receives a fencing token, and writes to a downstream
    register only if the token is greater than the register's current token."
    (:gen-class)
    (:require
      [cheshire.core :as json]
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
      [jepsen.tests :as tests]
      [jepsen.util :as util])
    (:import
      (java.io BufferedReader InputStreamReader OutputStreamWriter)
      (java.lang ProcessBuilder$Redirect)
      (java.util UUID)))

(def remote-binary "/opt/clavis/clavis")
(def uploaded-binary "/home/ubuntu/clavis")
(def data-root "/var/lib/clavis")
(def log-file "/var/log/clavis.log")
(def pid-file "/var/run/clavis.pid")

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
  (c/exec :sudo :bash :-lc (str/join " " parts)))

(defn sh!
  [& commands]
  (c/exec :sudo :bash :-lc (str/join " ; " commands)))

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
      (c/exec :sudo :bash :-lc
              (str "nohup " (str/join " " args)
                   " >> " log-file " 2>&1 & echo $! > " pid-file)))
    (Thread/sleep (if bootstrap? 3000 5000))))

(defrecord ClavisDB [binary]
  db/DB
  (setup! [_ test node]
    (info "installing clavis on" node)
    (sh "mkdir -p /opt/clavis")
    (c/upload binary uploaded-binary)
    (sh! (str "cp " uploaded-binary " " remote-binary)
         (str "chmod +x " remote-binary)
         (str "mkdir -p " data-root)
         (str "rm -rf " data-root "/*")
         (str ": > " log-file))
    (kill-clavis!)
    (start-clavis! test node))

  (teardown! [_ _ node]
    (info "stopping clavis on" node)
    (kill-clavis!))

  db/LogFiles
  (log-files [_ _ _]
    [log-file]))

(def registers
  "The downstream system: per resource, the highest token it has accepted."
  (atom {}))

(defn reset-registers!
  []
  (reset! registers {}))

(defn accept-write!
  "Stores value if token is higher than any token the resource has seen."
  [resource token value]
  (let [[old new] (swap-vals! registers
                              (fn [rs]
                                (if (< (get-in rs [resource :token] 0) token)
                                  (assoc rs resource {:token token :value value})
                                  rs)))]
    (not (identical? old new))))

(defn seeds
  [test]
  (str/join "," (map #(str % ":9000") (:nodes test))))

(defn start-holder!
  "Starts the Go holder process, which talks to Clavis through the SDK and keeps
  its lease alive with heartbeats. See jepsen/holder/main.go."
  [test args]
  (let [log (io/file (:holder-log test))
        _ (io/make-parents log)
        pb (doto (ProcessBuilder. ^java.util.List (into [(:holder test) "-seeds" (seeds test)
                                                         "-ttl" (str (:lease-ttl test) "s")]
                                                        args))
             (.redirectError (ProcessBuilder$Redirect/appendTo log)))
        p (.start pb)]
    {:process p
     :out (BufferedReader. (InputStreamReader. (.getInputStream p)))
     :in (OutputStreamWriter. (.getOutputStream p))}))

(defn read-event
  "The holder's next event, or nil if it exited."
  [h]
  (some-> (.readLine ^BufferedReader (:out h)) (json/parse-string true)))

(defn command!
  [h cmd]
  (doto ^OutputStreamWriter (:in h)
    (.write (str cmd "\n"))
    (.flush))
  (read-event h))

(defn stop-holder!
  [h]
  (.destroyForcibly ^Process (:process h)))

(defn signal-holder!
  [h signal]
  (shell/sh "kill" (str "-" signal) (str (.pid ^Process (:process h)))))

(defn complete
  [op type & {:as extra}]
  (assoc op :type type :value (merge (:value op) extra)))

(defn holder-error
  [ev]
  (if ev
    (str (:phase ev) ": " (:error ev))
    "holder exited without a result"))

(defrecord ClavisClient [node]
  client/Client
  (open! [this _ node]
    (assoc this :node node))

  (setup! [this test]
    ;; Wait until the cluster can open a session, so the first operations do
    ;; not fail just because a leader has not been elected yet.
    (let [deadline (+ (System/currentTimeMillis) 60000)]
      (loop []
        (let [h (start-holder! test ["-owner" (str "jepsen-probe-" node) "-probe"])
              ev (try (read-event h) (finally (stop-holder! h)))]
          (cond
            (= "ready" (:event ev)) this
            (< deadline (System/currentTimeMillis))
            (throw (ex-info "cluster did not become ready" {:node node :event ev}))
            :else (do (Thread/sleep 1000) (recur)))))))

  (invoke! [this test op]
    (let [{:keys [resource value pause? wait-ms hold-ms]} (:value op)
          h (start-holder! test ["-owner" (str "jepsen-" (:process op) "-" (UUID/randomUUID))
                                 "-lock" (str "resource:" resource)
                                 "-wait" (str wait-ms "ms")])]
      (try
        (let [ev (read-event h)
              acquired-at (util/relative-time-nanos)]
          (case (:event ev)
            "busy" (complete op :fail :error :busy)

            "acquired"
            (let [token (long (:token ev))]
              (Thread/sleep (long hold-ms))
              (let [checked (command! h "check")
                    valid-at (util/relative-time-nanos)]
                (cond
                  (not= "valid" (:event checked))
                  (complete op :fail :error :lost :token token)

                  pause?
                  ;; The client stalls between confirming it holds the lock
                  ;; and writing, long enough for its lease to expire.
                  (do (signal-holder! h "STOP")
                      (Thread/sleep (long (max (:stale-hold-ms test)
                                               (* 1000 (+ (:lease-ttl test) 2)))))
                      (signal-holder! h "CONT")
                      (if (accept-write! resource token value)
                        (complete op :ok :token token)
                        (complete op :fail :error :stale-token :token token)))

                  :else
                  (let [accepted? (accept-write! resource token value)]
                    (command! h "release")
                    (if accepted?
                      (complete op :ok :token token :held [acquired-at valid-at])
                      (complete op :fail :error :stale-token :token token
                                :held [acquired-at valid-at]))))))

            ;; Anything else leaves the outcome unknown: the lock may or may
            ;; not have been granted.
            (assoc op :type :info :error (holder-error ev))))
        (catch Exception e
          (assoc op :type :info :error (.getMessage e)))
        (finally
          (stop-holder! h)))))

  (teardown! [this _]
    this)

  (close! [_ _]
    nil))

(defn mutual-exclusion-violations
  "Ops that held a resource at overlapping times. An op's :held interval runs
  from when it got the lock to when its client last confirmed the session was
  alive, so any overlap means two clients held the lock at once."
  [ops]
  (->> (filter :held ops)
       (group-by :resource)
       vals
       (mapcat (fn [held]
                 (->> (sort-by (comp first :held) held)
                      (reduce (fn [{:keys [latest] :as acc} op]
                                (let [acc (if (and latest (< (first (:held op)) (second (:held latest))))
                                            (update acc :violations conj {:type :overlapping-holders
                                                                          :previous latest
                                                                          :current op})
                                            acc)]
                                  (if (or (nil? latest) (< (second (:held latest)) (second (:held op))))
                                    (assoc acc :latest op)
                                    acc)))
                              {:latest nil :violations []})
                      :violations)))))

(defrecord FencedRegisterChecker []
  checker/Checker
  (check [_ test history _]
    (let [completed (loop [pending {}
                           completed []
                           ops history]
                      (if-let [op (first ops)]
                        (let [op-key [(:process op) (:f op)]]
                          (cond
                           (= :invoke (:type op))
                           (recur (assoc pending op-key op) completed (rest ops))

                           (and (contains? #{:ok :fail} (:type op))
                                (= :fenced-write (:f op)))
                           (let [started (get pending op-key)]
                             (recur (dissoc pending op-key)
                               (conj completed
                                     (assoc (:value op)
                                            :result-type (:type op)
                                            :invoke-time (:time started)
                                            :complete-time (:time op)
                                            :complete-index (:index op)))
                               (rest ops)))

                           :else
                           (recur (dissoc pending op-key) completed (rest ops))))
                        completed))
          accepted (filterv #(= :ok (:result-type %)) completed)
          stale-rejections (filterv #(= :stale-token (:error %)) completed)
          token-results (filterv #(or (= :ok (:result-type %)) (:token %)) completed)
          grouped (group-by :resource accepted)
          minimum-successes (max 1 (long (or (:min-successful-ops test) 1)))
          missing-invocations (for [op token-results
                                    :when (nil? (:invoke-time op))]
                                {:type :missing-invocation
                                 :operation op})
          missing-tokens (for [op token-results
                               :when (nil? (:token op))]
                           {:type :missing-token
                            :operation op})
          duplicate-tokens (->> token-results
                                (filter :token)
                                (group-by :token)
                                (keep (fn [[token ops]]
                                        (when (> (count ops) 1)
                                          {:type :duplicate-token
                                           :token token
                                           :operations ops}))))
          realtime-order-violations
          (for [a token-results
                b token-results
                :when (and (number? (:token a))
                           (number? (:token b))
                           (number? (:complete-time a))
                           (number? (:invoke-time b))
                           (not= (:complete-index a) (:complete-index b))
                           (< (:complete-time a) (:invoke-time b))
                           (not (< (:token a) (:token b))))]
            {:type :non-monotonic-token
             :previous a
             :current b})
          ;; A client that had just confirmed its session was alive cannot be
          ;; fenced out unless someone else got the lock while it held it.
          stale-while-held (for [op stale-rejections
                                 :when (:held op)]
                             {:type :stale-while-held
                              :operation op})
          progress-violations
          (when (< (count accepted) minimum-successes)
            [{:type :insufficient-progress
              :accepted (count accepted)
              :minimum minimum-successes}])
          violations (vec (concat missing-invocations
                                  missing-tokens
                                  duplicate-tokens
                                  realtime-order-violations
                                  (mutual-exclusion-violations completed)
                                  stale-while-held
                                  progress-violations))]
      {:valid? (empty? violations)
       :accepted-count (count accepted)
       :stale-rejection-count (count stale-rejections)
       :held-count (count (filter :held completed))
       :lost-count (count (filter #(= :lost (:error %)) completed))
       :resource-count (count grouped)
       :minimum-successful-ops minimum-successes
       :violations violations})))

(defn op
  [test]
  (let [ttl-ms (* 1000 (:lease-ttl test))]
    {:type :invoke
     :f :fenced-write
     :value {:resource (str "resource-" (rand-int (:resources test)))
             :value (str (UUID/randomUUID))
             :pause? (< (rand) (:stale-probability test))
             ;; Waiting ops queue on the leader for a busy lock.
             :wait-ms (if (< (rand) (:wait-probability test)) (:wait-ms test) 0)
             ;; Long holds outlast the lease TTL, so they depend on renewals.
             :hold-ms (if (< (rand) (:long-hold-probability test))
                        (+ ttl-ms (rand-int ttl-ms))
                        (rand-int (inc (:hold-ms test))))}}))

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
   [nil "--holder PATH" "Path to the holder client built from jepsen/holder."
    :default "./clavis-holder"]
   [nil "--holder-log PATH" "File that collects the holder clients' SDK logs."
    :default "store/holder.log"]
   [nil "--lease-ttl SECONDS" "Clavis lease TTL used by Jepsen operations."
    :default 6
    :parse-fn parse-long]
   [nil "--resources N" "Number of logical resources to coordinate."
    :default 3
    :parse-fn parse-long]
   [nil "--rate HZ" "Approximate client operation rate."
    :default 5.0
    :parse-fn #(Double/parseDouble %)]
   [nil "--hold-ms MS" "Longest short hold before a fenced write."
    :default 100
    :parse-fn parse-long]
   [nil "--long-hold-probability P" "Probability that an operation holds its lock for one to two lease TTLs."
    :default 0.3
    :parse-fn #(Double/parseDouble %)]
   [nil "--wait-probability P" "Probability that an operation waits in line for a busy lock."
    :default 0.5
    :parse-fn #(Double/parseDouble %)]
   [nil "--wait-ms MS" "How long a waiting operation waits for a busy lock."
    :default 8000
    :parse-fn parse-long]
   [nil "--stale-hold-ms MS" "Paused-holder sleep before attempting a stale write."
    :default 10000
    :parse-fn parse-long]
   [nil "--stale-probability P" "Probability that an operation simulates a paused holder."
    :default 0.05
    :parse-fn #(Double/parseDouble %)]
   [nil "--min-successful-ops N" "Minimum successful fenced writes required for a valid run."
    :default 1
    :parse-fn parse-long]
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
          :generator (workload opts)}))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn clavis-test
                                  :usage (cli/test-usage)
                                  :opt-spec cli-opts})
            args))
