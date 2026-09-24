(ns grete.core
  (:require [clojure.tools.logging :as log]
            [clojure.string :as s]
            [grete.gregor :as gregor]
            [grete.scheduler :as sch]
            [grete.tools :as t])
  (:import [java.time Duration]))

(defn to-prop [k]
  (-> k name (s/replace #"-" ".")))

(defn to-props
  "ranames keys by converting them to strings and substituting dashes with periods
   only does top level keys"
  [conf]
  (into {}
    (for [[k v] conf]
      [(to-prop k) v])))

(defn producer [{:keys [bootstrap-servers] :as conf}]
  (let [props (-> (dissoc conf :bootstrap-servers
                               :topics)
                  to-props)]
    (gregor/producer bootstrap-servers
                     props)))

(defn send!
  "dummy gregor send wrap to:
    1. give it a '!'
    2. avoid requiring another ns to 'send'"
  ([producer topic msg]
   (gregor/send producer topic msg))
  ([producer topic key msg]
   (gregor/send producer topic key msg)))

(defn send-then!
  "dummy gregor send-then wrap to:
    1. give it a '!'
    2. avoid requiring another ns to 'send-then'"
  ([producer topic msg then]
   (gregor/send-then producer topic msg then))
  ([producer topic key msg then]
   (gregor/send-then producer topic key msg then)))

(defn close [producer]
  (gregor/close producer))

(defn- edn-to-consumer [{:keys [bootstrap-servers
                                group-id
                                topics] :as conf}]
  [bootstrap-servers
   group-id
   topics
   (to-props (dissoc conf :topics))])

;; consuming..

(defn consumer-records->maps [cs]
  (-> (map gregor/consumer-record->map cs)
      seq))

(defn poll
  "fetches sequetially from the last consumed offset
   return 'org.apache.kafka.clients.consumer.ConsumerRecords' currently available to the consumer (via a single poll)
   if a 'timeout' param is 0, returns immediately with any records that are available now."
  ([consumer] (poll consumer 100))
  ([consumer timeout]
   (.poll consumer (Duration/ofMillis timeout))))

(defn consumer [conf]
  (log/info "consumer config:" (t/cloak-secrets conf))
  (->> (edn-to-consumer conf)
       (apply gregor/consumer)))

(defn default-on-error
  [{:keys [phase error]}]
  (log/errorf "kafka: could not consume a message: %s phase: %s"
              error
              (or phase :unknown)))

(defn- resolve-error-handlers
  "picks a handler per phase, falling back to ':on-error', then to the default"
  [{:keys [on-error on-poll-error on-process-error on-commit-error]}]
  (let [on-error (or on-error default-on-error)]
    {:poll    (or on-poll-error on-error)
     :process (or on-process-error on-error)
     :commit  (or on-commit-error on-error)}))

(defn- run-phase
  "runs a single consumer phase, reporting a failure to that phase's handler.
   returns '::failed' when the phase throws, so the caller can stop early"
  [handlers context phase thunk]
  (try
    (thunk)
    (catch Throwable error
      (try
        ((get handlers phase) (assoc context
                                     :phase phase
                                     :error error))
        (catch Throwable handler-error
          (log/error "kafka: consumer error handler failed" handler-error)))
      ::failed)))

(defn- consume-once
  [consumer process ms n handlers]
  (let [context          {:consumer        consumer
                          :consumer-number n}
        run              (partial run-phase handlers)
        consumer-records (run context :poll #(poll consumer ms))]
    (when (and consumer-records
               (not= ::failed consumer-records))
      (let [result (run context :process #(process consumer consumer-records))]
        (when-not (= ::failed result)
          (run (assoc context :result result) :commit #(gregor/commit-offsets! consumer)))))))

(defn consume
  "the 'process' function will take 'org.apache.kafka.clients.consumer.ConsumerRecords'
   which can be turns to a seq of maps with 'consumer-records->maps'"
  ([consumer process running? ms n]
   (consume consumer process running? ms n {}))
  ([consumer process running? ms n options]
   (let [handlers (resolve-error-handlers options)]
     (log/info "starting" (inc n) "consumer")
     (while @running?
       (consume-once consumer process ms n handlers))
     (gregor/close consumer))))

(defn run-consumers
  ([process conf]
   (run-consumers process conf {}))
  ([process {:keys [threads poll-ms] :as conf} options]
   (let [running? (atom true)
         pool (sch/new-executor "kafka consumers" (if (number? threads)
                                                    threads
                                                    42))]
     (dotimes [t threads]
       (let [c (consumer (dissoc conf :threads :poll-ms))]
         (log/info "subscribing to:" (gregor/subscription c))
         (.submit pool #(consume c process running? poll-ms t options))))
     (log/info "started" threads "consumers ->"
               (t/cloak-secrets conf))
     {:pool pool :running? running?})))

(defn stop-consumers [{:keys [pool running?]}]
  (reset! running? false)
  (.shutdown pool))

(defn offsets [c]
  (for [tp (gregor/assignment c)]
    (let [p (.partition tp)
          t (.topic tp)]
      {:topic t :partition p :offset (gregor/committed c t p)})))

(defn reset-offsets [c topic pnum]
  (let [offsets (reduce (fn [ofs p]
                          (conj ofs {:topic topic
                                     :partition p
                                     :offset 0})) [] (range pnum))]
    (gregor/commit-offsets! c offsets)))
