;;   Copyright (c) Rich Hickey and contributors. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.

(ns ^{:skip-wiki true}
clojure.core.async.impl.channels
  (:require [clojure.core.async.impl.protocols :as impl]
            [clojure.core.async.impl.dispatch :as dispatch]
            [clojure.core.async.impl.mutex :as mutex]
            [if.core :refer :all])
  (:import [java.util LinkedList Queue Iterator]
           [java.util.concurrent.locks Lock]))

(set! *warn-on-reflection* true)

(defmacro assert-unlock [lock test msg]
  `(when-not ~test
     (.unlock ~lock)
     (throw (new AssertionError (str "Assert failed: " ~msg "\n" (pr-str '~test))))))

(defn box [val]
  (reify clojure.lang.IDeref
    (deref [_] val)))

(defprotocol MMC
  (cleanup [_])
  (abort [_]))

(deftype ManyToManyChannel [^LinkedList takes               ;; List of pending take requests
                            ^LinkedList puts                ;; List of pending put requests
                            ^Queue buf                      ;; Buffer for holding put val's
                            closed                          ;; Atom for setting closed boolean
                            ^Lock mutex
                            add!                            ;; fn to add a val to the buf
                            ]
  MMC
  (cleanup
    ;; Remove all entries from takes and puts that are not active.
    [_]
    (when-not (.isEmpty takes)
      (let [iter (.iterator takes)]
        (loop [taker (.next iter)]
          (when-not (impl/active? taker)
            (.remove iter))
          (when (.hasNext iter)
            (recur (.next iter))))))
    (when-not (.isEmpty puts)
      (let [iter (.iterator puts)]
        (loop [[putter] (.next iter)]
          (when-not (impl/active? putter)
            (.remove iter))
          (when (.hasNext iter)
            (recur (.next iter)))))))

  (abort
    [this]
    (let [iter (.iterator puts)]
      (when (.hasNext iter)
        (loop [^Lock putter (.next iter)]
          (.lock putter)
          (let [put-cb (and (impl/active? putter) (impl/commit putter))]
            (.unlock putter)
            (when put-cb
              (dispatch/run (fn [] (put-cb true))))
            (when (.hasNext iter)
              (recur (.next iter)))))))
    (.clear puts)
    (impl/close! this))

  impl/WritePort
  (put!
    [this val handler]
    (when (nil? val)
      (throw (IllegalArgumentException. "Can't put nil on channel")))
    (.lock mutex)
    (cleanup this)
    (if @closed
      (then (.unlock mutex)
          (box false))
      (let [^Lock handler handler]
        (if (and buf (not (impl/full? buf)) (not (.isEmpty takes)))
          ;; If-Then (there is a buffer, buffer has capacity and there is a taker ready)
          (then
            (.lock handler)
            (let [put-cb (and (impl/active? handler) (impl/commit handler))]
              (.unlock handler)
              (if put-cb
                (let [done? (reduced? (add! buf val))]      ;; Add the val to the buf
                  (if (pos? (count buf))
                    (let [iter (.iterator takes)

                          ;; Create a seq take callbacks that can be invoked without args.
                          take-cbs (loop [takers []]
                                     (if (and (.hasNext iter) (pos? (count buf)))
                                       (let [^Lock taker (.next iter)]
                                         (.lock taker)
                                         (let [ret (and (impl/active? taker) (impl/commit taker))]
                                           (.unlock taker)
                                           (if ret
                                             (let [val (impl/remove! buf)]
                                               (.remove iter)
                                               (recur (conj
                                                        takers
                                                        ;; New fn that will invoke the take fn with the val arg
                                                        (fn [] (ret val)))))
                                             (recur takers))))
                                       takers))]
                      (if (seq take-cbs)
                        (do
                          (when done?
                            (abort this))
                          (.unlock mutex)
                          (doseq [f take-cbs]
                            ;; Invoke the take fns on the dispatcher
                            (dispatch/run f)))
                        (do
                          (when done?
                            (abort this))
                          (.unlock mutex))))
                    (do
                      (when done?
                        (abort this))
                      (.unlock mutex)))
                  (box true))
                (do (.unlock mutex)
                    nil))))
          (else
            ;; If-Else (There is no buffer or no taker)
            (let [iter (.iterator takes)
                  ;; Get a pair of the put and take callbacks.
                  [put-cb take-cb] (when (.hasNext iter)
                                     (loop [^Lock taker (.next iter)]
                                       (if (< (impl/lock-id handler) (impl/lock-id taker))
                                         (do (.lock handler) (.lock taker))
                                         (do (.lock taker) (.lock handler)))
                                       (let [ret (when (and (impl/active? handler) (impl/active? taker))
                                                   [(impl/commit handler) (impl/commit taker)])]
                                         (.unlock handler)
                                         (.unlock taker)
                                         (if ret
                                           (do
                                             (.remove iter)
                                             ret)
                                           (when (.hasNext iter)
                                             (recur (.next iter)))))))]
              (if (and put-cb take-cb)
                ;; If-Then both put and take callbacks are valid. Give the val to the taker.
                (then
                  (.unlock mutex)
                  (dispatch/run (fn [] (take-cb val)))
                  (box true))
                ;; If-Else take callback is not present. put optionally has a No-Op callback.
                (else
                  (if (and buf (not (impl/full? buf)))
                    ;; If-Then buf exists and has space.
                    (do
                      (.lock handler)
                      (let [put-cb (and (impl/active? handler) (impl/commit handler))]
                        (.unlock handler)
                        (if put-cb
                          (let [done? (reduced? (add! buf val))] ;; Add val to the buffer
                            (when done?
                              (abort this))
                            (.unlock mutex)
                            (box true))
                          (do (.unlock mutex)
                              nil))))
                    ;; If-Else No buffer or is full.
                    (do
                      (when (and (impl/active? handler) (impl/blockable? handler))
                        (assert-unlock mutex
                                       (< (.size puts) impl/MAX-QUEUE-SIZE)
                                       (str "No more than " impl/MAX-QUEUE-SIZE
                                            " pending puts are allowed on a single channel."
                                            " Consider using a windowed buffer."))
                        ;; Add the put handler and val to the puts list
                        (.add puts [handler val]))
                      (.unlock mutex)
                      nil))))))))))

  impl/ReadPort
  (take!
    [this handler]
    (.lock mutex)
    (cleanup this)
    (let [^Lock handler handler
          commit-handler (fn []
                           (.lock handler)
                           (let [take-cb (and (impl/active? handler) (impl/commit handler))]
                             (.unlock handler)
                             take-cb))]
      (if (and buf (pos? (count buf)))
        ;; If-Then There is a buffer and has some elements
        (then
          (if-let [take-cb (commit-handler)]
            (then
                (let [val (impl/remove! buf)                    ;; Get a single val from the buffer
                    iter (.iterator puts)
                    [done? cbs] (when (.hasNext iter)
                                  (loop [cbs []
                                         [^Lock putter val] (.next iter)]
                                    (.lock putter)
                                    (let [cb (and (impl/active? putter) (impl/commit putter))]
                                      (.unlock putter)
                                      (.remove iter)
                                      (let [cbs (if cb (conj cbs cb) cbs)
                                            ;; Move a callback from the puts list to cbs based
                                            done? (when cb (reduced? (add! buf val)))]
                                        (if (and (not done?) (not (impl/full? buf)) (.hasNext iter))
                                          ;; Recur if the buffer has more capacity
                                          (recur cbs (.next iter))
                                          [done? cbs])))))]
                (when done?
                  (abort this))
                (.unlock mutex)
                (doseq [cb cbs]
                  ;; Invoke the puts callbacks that can be run now.
                  (dispatch/run #(cb true)))
                (box val)))
            (else (.unlock mutex)
                nil)))
        ;; If-Else There is no buffer or it is empty
        (else
          (let [iter (.iterator puts)
                [take-cb put-cb val] (when (.hasNext iter)
                                       (loop [[^Lock putter val] (.next iter)]
                                         (if (< (impl/lock-id handler) (impl/lock-id putter))
                                           (do (.lock handler) (.lock putter))
                                           (do (.lock putter) (.lock handler)))
                                         (let [ret (when (and (impl/active? handler) (impl/active? putter))
                                                     [(impl/commit handler) (impl/commit putter) val])]
                                           (.unlock handler)
                                           (.unlock putter)
                                           (if ret
                                             (then
                                               (.remove iter)
                                               ret)
                                             (else
                                               (when-not (impl/active? putter)
                                                 (.remove iter)
                                                 (when (.hasNext iter)
                                                   (recur (.next iter)))))))))]
            (if (and put-cb take-cb)
              ;; If-Then There is a put and take callback.
              (then
                (.unlock mutex)
                ;; Invoke the put callback
                (dispatch/run #(put-cb true))
                ;; Return the val to be executed as part of the take callback
                (box val))
              ;; If-Else put callback is not there. Take always has a callback.
              (else
                (if @closed
                  ;; If-Then closed
                  (then
                    (when buf (add! buf))
                    (let [has-val (and buf (pos? (count buf)))]
                      (if-let [take-cb (commit-handler)]
                        (let [val (when has-val (impl/remove! buf))]
                          (.unlock mutex)
                          (box val))
                        (do
                          (.unlock mutex)
                          nil))))
                  ;; If-Else not closed
                  (else
                    (when (impl/blockable? handler)
                      (assert-unlock mutex
                                     (< (.size takes) impl/MAX-QUEUE-SIZE)
                                     (str "No more than " impl/MAX-QUEUE-SIZE
                                          " pending takes are allowed on a single channel."))
                      ;; Add the handler callback function to the takes list
                      (.add takes handler))
                    (.unlock mutex)
                    nil)))))))))

  impl/Channel
  (closed? [_] @closed)
  (close!
    [this]
    (.lock mutex)
    (cleanup this)
    (if @closed
      (then
        (.unlock mutex)
        nil)
      (else
        (reset! closed true)
        (when (and buf (.isEmpty puts))
          (add! buf))
        (let [iter (.iterator takes)]
          (when (.hasNext iter)
            (loop [^Lock taker (.next iter)]
              (.lock taker)
              (let [take-cb (and (impl/active? taker) (impl/commit taker))]
                (.unlock taker)
                (when take-cb
                  (let [val (when (and buf (pos? (count buf))) (impl/remove! buf))]
                    (dispatch/run (fn [] (take-cb val)))))
                (.remove iter)
                (when (.hasNext iter)
                  (recur (.next iter)))))))
        (when buf (impl/close-buf! buf))
        (.unlock mutex)
        nil))))

(defn- ex-handler [ex]
  (-> (Thread/currentThread)
      .getUncaughtExceptionHandler
      (.uncaughtException (Thread/currentThread) ex))
  nil)

(defn- handle [buf exh t]
  (let [else ((or exh ex-handler) t)]
    (if (nil? else)
      buf
      (impl/add! buf else))))

(defn chan
  ([buf] (chan buf nil))
  ([buf xform] (chan buf xform nil))
  ([buf xform exh]
   (ManyToManyChannel.
     (LinkedList.)
     (LinkedList.)
     buf
     (atom false)
     (mutex/mutex)
     (let [add! (if xform (xform impl/add!) impl/add!)]
       (fn
         ([buf]
          (try
            (add! buf)
            (catch Throwable t
              (handle buf exh t))))
         ([buf val]
          (try
            (add! buf val)
            (catch Throwable t
              (handle buf exh t)))))))))

