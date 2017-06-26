(ns clojure.core.async_scenarios
  (:refer-clojure :exclude [map into reduce transduce merge take partition partition-by])
  (:require [clojure.core.async.impl.buffers :as b]
            [clojure.core.async :refer :all :as a]
            [clojure.test :refer :all]))

(defn print-put [val]
  (let [ret (str "put! fn invoked " (str "<" val ">"))]
    (println ret)
    ret))

(defn print-take [val]
  (let [ret (str "take! fn invoked " (str "<" val ">"))]
    (println ret)
    ret))

(deftest test-channels-without-buffer
  ;; Take and put without a buffer'ed channel
  (let [val "val"
        c (chan)]
    (take! c print-take)
    (put! c val print-put))

  ;; Put and Take without a buffer'ed channel
  (let [val 42
        c (chan)]
    (put! c val print-put)
    (take! c print-take)))

(deftest test-channels-with-buffer
  ;; Take and put with a buffer'ed channel
  (let [val "a val"
        c (chan 1)]
    (take! c print-take)
    (put! c val print-put))

  ;; Put and Take with a buffer'ed channel
  (let [val "test"
        c (chan 1)]
    (put! c val print-put)
    (take! c print-take)))

(deftest test-channels-with-buffer-overflow
  ;; Take and put with a buffer'ed channel
  (let [val 1
        c (chan 1)]
    (take! c print-take)
    (take! c print-take)
    (put! c val print-put)
    (put! c (+ 1 val) print-put))

  ;; Put and Take with a buffer'ed channel
  (let [val 5
        c (chan 1)]
    (put! c val print-put)
    (put! c (+ 1 val) print-put)
    (take! c print-take)
    (take! c print-take)))