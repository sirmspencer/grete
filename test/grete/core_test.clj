(ns grete.core-test
  (:require [clojure.test :refer [deftest is]]
            [grete.core :as core]
            [grete.gregor :as gregor]))

(deftest on-error-receives-consumer-context
  (let [running? (atom true)
        consumer (Object.)
        config   {:group-id "group" :topics ["topic"]}
        error    (ex-info "failure" {})
        received (atom nil)]
    (with-redefs [core/poll              (fn [_ _] (throw error))
                  gregor/close           (fn [_] (reset! running? false))
                  gregor/commit-offsets! (fn [& _] nil)]
      (core/consume consumer
                    (fn [& _] nil)
                    running?
                    0
                    0
                    {:conf config
                     :on-error (fn [context]
                                 (reset! received context)
                                 (reset! running? false))}))
    (is (= {:consumer consumer
            :conf     config
            :error    error}
           @received))))

(deftest on-error-failure-does-not-prevent-consumer-close
  (let [running? (atom true)
        closed?  (atom false)]
    (with-redefs [core/poll              (fn [_ _] (throw (ex-info "failure" {})))
                  gregor/close           (fn [_] (reset! closed? true))
                  gregor/commit-offsets! (fn [& _] nil)]
      (core/consume (Object.)
                    (fn [& _] nil)
                    running?
                    0
                    0
                    {:on-error (fn [_]
                                 (reset! running? false)
                                 (throw (ex-info "handler failure" {})))}))
    (is (true? @closed?))))
