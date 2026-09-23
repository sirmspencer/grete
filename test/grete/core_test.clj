(ns grete.core-test
  (:require [clojure.test :refer [deftest is]]
            [grete.core :as core]
            [grete.gregor :as gregor]))

(deftest phase-specific-handlers-receive-phase-context
  (let [running?     (atom true)
        received     (atom nil)
        commit-error (ex-info "commit failure" {})
        consumer     (Object.)]
    (with-redefs [core/poll              (fn [_ _] (Object.))
                  gregor/close           (fn [_] (reset! running? false))
                  gregor/commit-offsets! (fn [& _] (throw commit-error))]
      (core/consume consumer
                    (fn [& _] :processed)
                    running?
                    0
                    7
                    {:on-commit-error (fn [context]
                                        (reset! received context)
                                        (reset! running? false))}))
    (is (= {:consumer        consumer
            :consumer-number 7
            :phase           :commit
            :result          :processed
            :error           commit-error}
           @received))))

(deftest phase-specific-handler-takes-precedence
  (let [running? (atom true)
        received (atom [])]
    (with-redefs [core/poll    (fn [_ _] (throw (ex-info "poll failure" {})))
                  gregor/close (fn [_] nil)]
      (core/consume (Object.)
                    (fn [& _] (swap! received conj :process))
                    running?
                    0
                    0
                    {:on-error      (fn [_] (swap! received conj :generic))
                     :on-poll-error (fn [context]
                                      (swap! received conj (:phase context))
                                      (reset! running? false))}))
    (is (= [:poll] @received))))

(deftest process-errors-do-not-commit
  (let [running?  (atom true)
        committed? (atom false)]
    (with-redefs [core/poll              (fn [_ _] (Object.))
                  gregor/close           (fn [_] nil)
                  gregor/commit-offsets! (fn [& _] (reset! committed? true))]
      (core/consume (Object.)
                    (fn [& _] (throw (ex-info "process failure" {})))
                    running?
                    0
                    0
                    {:on-process-error (fn [_] (reset! running? false))}))
    (is (false? @committed?))))

(deftest nil-process-results-still-commit
  (let [running?  (atom true)
        committed? (atom false)]
    (with-redefs [core/poll              (fn [_ _] (Object.))
                  gregor/close           (fn [_] (reset! running? false))
                  gregor/commit-offsets! (fn [& _]
                                           (reset! committed? true)
                                           (reset! running? false))]
      (core/consume (Object.)
                    (fn [& _] nil)
                    running?
                    0
                    0
                    {}))
    (is (true? @committed?))))

(deftest error-handler-failure-does-not-prevent-close
  (let [running? (atom true)
        closed?  (atom false)]
    (with-redefs [core/poll    (fn [_ _] (throw (ex-info "poll failure" {})))
                  gregor/close (fn [_] (reset! closed? true))]
      (core/consume (Object.)
                    (fn [& _] nil)
                    running?
                    0
                    0
                    {:on-error (fn [_]
                                 (reset! running? false)
                                 (throw (ex-info "handler failure" {})))}))
    (is (true? @closed?))))
