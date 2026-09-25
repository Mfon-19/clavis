(ns clavis.jepsen-test
    (:require
      [clojure.test :refer [deftest is testing]]
      [jepsen.checker :as checker]
      [jepsen.client :as client]
      [clavis.jepsen :as clavis]))

(deftest fenced-register-accepts-only-increasing-tokens
  (clavis/reset-registers!)
  (is (true? (clavis/accept-write! "resource-a" 1 "first")))
  (is (false? (clavis/accept-write! "resource-a" 1 "stale")))
  (is (false? (clavis/accept-write! "resource-a" 0 "older")))
  (is (true? (clavis/accept-write! "resource-a" 2 "second")))
  (is (= {:token 2 :value "second"} (get @clavis/registers "resource-a"))))

(deftest fenced-register-is-safe-under-concurrent-writers
  (clavis/reset-registers!)
  (let [accepted (->> (range 1 201)
                      (pmap #(when (clavis/accept-write! "r" % (str %)) %))
                      (remove nil?)
                      doall)]
    (is (= 200 (get-in @clavis/registers ["r" :token])))
    (is (= 200 (last (sort accepted))))))

(deftest client-open-keeps-client-record
  (let [opened (client/open! (clavis/->ClavisClient nil) {} "n1")]
    (is (satisfies? client/Client opened))
    (is (= "n1" (:node opened)))))

(defn check
  [history]
  (checker/check (clavis/->FencedRegisterChecker) nil history nil))

(deftest checker-accepts-a-valid-history
  (let [result (check [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a" :pause? true}}
                       {:time 2 :index 2 :type :invoke :process 1 :f :fenced-write :value {:resource "a"}}
                       {:time 3 :index 3 :type :ok :process 1 :f :fenced-write :value {:resource "a" :token 2 :held [2 3]}}
                       {:time 4 :index 4 :type :fail :process 0 :f :fenced-write :value {:resource "a" :pause? true :token 1 :error :stale-token}}
                       {:time 5 :index 5 :type :invoke :process 1 :f :fenced-write :value {:resource "a"}}
                       {:time 6 :index 6 :type :ok :process 1 :f :fenced-write :value {:resource "a" :token 3 :held [5 6]}}
                       {:time 7 :index 7 :type :invoke :process 1 :f :fenced-write :value {:resource "b"}}
                       {:time 8 :index 8 :type :fail :process 1 :f :fenced-write :value {:resource "b" :error :busy}}])]
    (is (true? (:valid? result)))
    (is (= 2 (:accepted-count result)))
    (is (= 1 (:stale-rejection-count result)))
    (is (empty? (:violations result)))))

(deftest checker-rejects-reused-and-non-monotonic-tokens
  (let [result (check [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                       {:time 2 :index 2 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 7}}
                       {:time 3 :index 3 :type :invoke :process 0 :f :fenced-write :value {:resource "b"}}
                       {:time 4 :index 4 :type :ok :process 0 :f :fenced-write :value {:resource "b" :token 7}}])]
    (is (false? (:valid? result)))
    (is (some #(= :duplicate-token (:type %)) (:violations result)))
    (is (some #(= :non-monotonic-token (:type %)) (:violations result)))))

(deftest checker-rejects-overlapping-holders
  (testing "two clients that both confirmed they held the lock at the same time"
    (let [result (check [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                         {:time 2 :index 2 :type :invoke :process 1 :f :fenced-write :value {:resource "a"}}
                         {:time 9 :index 3 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 1 :held [2 8]}}
                         {:time 9 :index 4 :type :ok :process 1 :f :fenced-write :value {:resource "a" :token 2 :held [5 9]}}])]
      (is (false? (:valid? result)))
      (is (some #(= :overlapping-holders (:type %)) (:violations result)))))
  (testing "holders of different resources may overlap"
    (let [result (check [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                         {:time 2 :index 2 :type :invoke :process 1 :f :fenced-write :value {:resource "b"}}
                         {:time 9 :index 3 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 1 :held [2 8]}}
                         {:time 9 :index 4 :type :ok :process 1 :f :fenced-write :value {:resource "b" :token 2 :held [5 9]}}])]
      (is (true? (:valid? result))))))

(deftest checker-rejects-a-fenced-out-holder
  (let [result (check [{:time 1 :index 1 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                       {:time 2 :index 2 :type :ok :process 0 :f :fenced-write :value {:resource "a" :token 5 :held [1 2]}}
                       {:time 3 :index 3 :type :invoke :process 0 :f :fenced-write :value {:resource "a"}}
                       {:time 4 :index 4 :type :fail :process 0 :f :fenced-write
                        :value {:resource "a" :token 6 :held [3 4] :error :stale-token}}])]
    (is (false? (:valid? result)))
    (is (some #(= :stale-while-held (:type %)) (:violations result)))))

(deftest checker-requires-progress
  (let [result (check [])]
    (is (false? (:valid? result)))
    (is (= :insufficient-progress (-> result :violations first :type)))))
