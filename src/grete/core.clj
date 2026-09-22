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
  (log/error "kafka: could not consume a message"
             error
             "phase:"
             (or phase :unknown)))

(defn consume
  "the 'process' function will take 'org.apache.kafka.clients.consumer.ConsumerRecords'
   which can be turns to a seq of maps with 'consumer-records->maps'"
  ([consumer process running? ms n]
   (consume consumer process running? ms n {}))
  ([consumer process running? ms n {:keys [on-error
                                           on-poll-error
                                           on-process-error
                                           on-commit-error]}]
   (let [on-error         (or on-error default-on-error)
         on-poll-error    (or on-poll-error on-error)
         on-process-error (or on-process-error on-error)
         on-commit-error  (or on-commit-error on-error)
         report-error     (fn [handler context]
                            (try
                              (handler context)
                              (catch Throwable callback-error
                                (log/error "kafka: consumer error handler failed"
                                           callback-error))))]
     (log/info "starting" (inc n) "consumer")
     (while @running?
       (let [consumer-records (try
                                (poll consumer ms)
                                (catch Throwable error
                                  (report-error on-poll-error
                                                {:consumer        consumer
                                                 :consumer-number n
                                                 :phase           :poll
                                                 :error           error})
                                   nil))]
         (when consumer-records
           (let [process-result (try
                                  (process consumer consumer-records)
                                  (catch Throwable error
                                    (report-error on-process-error
                                                  {:consumer        consumer
                                                   :consumer-number n
                                                   :phase           :process
                                                   :error           error})
                                    ::process-failed))]
             (when-not (= ::process-failed process-result)
               (try
                 (gregor/commit-offsets! consumer)
                 (catch Throwable error
                   (report-error on-commit-error
                                 {:consumer        consumer
                                  :consumer-number n
                                  :phase           :commit
                                  :process-result  process-result
                                  :error           error}))))))))
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
