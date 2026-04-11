(ns clavis.jepsen-test
    (:require
      [clojure.test :refer [deftest is testing]]
      [jepsen.checker :as checker]
      [jepsen.client :as client]
      [clavis.jepsen :as clavis]))

(deftest field-handles-protobuf-json-key-shapes
  (testing "snake_case, camelCase, keyword, and string keys"
           (is (= 7 (clavis/field {:lease_id 7} :lease_id)))
           (is (= 7 (clavis/field {:leaseId 7} :lease_id)))
           (is (= 7 (clavis/field {"lease_id" 7} :lease_id)))
           (is (= 7 (clavis/field {"leaseId" 7} :lease_id)))))

(deftest long-value-parses-json-numeric-forms
  (is (= 42 (clavis/long-value 42)))
  (is (= 42 (clavis/long-value 42.0)))
  (is (= 42 (clavis/long-value "42"))))

(deftest fenced-register-accepts-only-increasing-tokens
  (let [registers (doto (java.io.File/createTempFile "clavis-jepsen-registers" ".edn")
                        (.deleteOnExit))
        lock (doto (java.io.File/createTempFile "clavis-jepsen-registers" ".lock")
                   (.deleteOnExit))]
    (binding [clavis/*register-path* (.getAbsolutePath registers)
              clavis/*register-lock-path* (.getAbsolutePath lock)]
             (clavis/reset-registers!)
             (is (true? (clavis/accept-write! "resource-a" 1 "first")))
             (is (false? (clavis/accept-write! "resource-a" 1 "stale")))
             (is (false? (clavis/accept-write! "resource-a" 0 "older")))
             (is (true? (clavis/accept-write! "resource-a" 2 "second")))
             (is (= {:token 2 :value "second"} (get (clavis/read-registers) "resource-a"))))))

(deftest client-open-keeps-client-record
  (let [opened (client/open! (clavis/->ClavisClient nil) {} "n1")]
    (is (satisfies? client/Client opened))
    (is (= "n1" (:node opened)))))

(deftest fenced-register-checker-accepts-monotonic-token-history
  (let [history [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                 {:time 2 :index 2 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 1}}
                 {:time 3 :index 3 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                 {:time 4 :index 4 :type :fail :process 0 :f :fenced-write :value {:resource "a" :token 1 :error :stale-token}}
                 {:time 5 :index 5 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                 {:time 6 :index 6 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 2}}
                 {:time 7 :index 7 :type :invoke :process 0 :f :fenced-write :value {:resource "b"}}
                 {:time 8 :index 8 :type :ok :process 0 :f :fenced-write :value {:resource "b" :token 1}}]
        result (checker/check (clavis/->FencedRegisterChecker) nil history nil)]
    (is (true? (:valid? result)))
    (is (= 3 (:accepted-count result)))
    (is (empty? (:violations result)))))

(deftest fenced-register-checker-rejects-non-monotonic-token-history
  (let [history [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                 {:time 2 :index 2 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 2}}
                 {:time 3 :index 3 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                 {:time 4 :index 4 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 2}}]
        result (checker/check (clavis/->FencedRegisterChecker) nil history nil)]
    (is (false? (:valid? result)))
    (is (= 1 (count (:violations result))))))
