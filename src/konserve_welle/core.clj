(ns konserve-welle.core
  (:require [clojurewerkz.welle.core :as wc]
            [clojurewerkz.welle.buckets :as wb]
            [clojurewerkz.welle.kv      :as kv]
            [clojure.core.async :as async :refer [<!! chan close! go put!]]
            [clojure.java.io :as io]
            [hasch.core :refer [uuid]]
            [konserve
             [protocols :refer [-bassoc -bget -deserialize -exists? -get-in -serialize -update-in
                                PBinaryAsyncKeyValueStore PEDNAsyncKeyValueStore]]
             [serializers :as ser]]
            [konserve.core :as k])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))


(defrecord WelleStore [conn conn-url bucket-opts serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [id (str (uuid key))
          res (chan)
          vals (:result (kv/fetch conn (:name bucket-opts) id))]
      (if (> (count vals) 1)
        (put! res (ex-info "This key has a conflict." {:type :multiple-values
                                                       :key key}))
        (put! res (not (nil? (first vals)))))
      (close! res)
      res))

  (-get-in [this key-vec]
    (let [[fkey & rkey] key-vec
          id (str (uuid fkey))
          vals (:result (kv/fetch conn (:name bucket-opts) id))]
      (cond (zero? (count vals))
            (go nil)

            (> (count vals) 1)
            (go (ex-info "This key has a conflict." {:type :multiple-values
                                                     :key fkey
                                                     :vals vals}))

            :else
            (let [res-ch (chan)]
              (try
                (let [bais (ByteArrayInputStream. (:value (first vals)))]
                  (when-let [res (get-in
                                  (second (-deserialize serializer read-handlers bais))
                                  rkey)]
                    (put! res-ch res)))
                res-ch
                (catch Exception e
                  (put! res-ch (ex-info "Could not read key."
                                        {:type :read-error
                                         :key fkey
                                         :exception e}))
                  res-ch)
                (finally
                  (close! res-ch)))))))

  (-update-in [this key-vec up-fn]
    (let [[fkey & rkey] key-vec
          id (str (uuid fkey))]
      (let [res-ch (chan)
            vals (:result (kv/fetch conn (:name bucket-opts) id))]
        (if (> (count vals) 1)
          (go (ex-info "This key has a conflict." {:type :multiple-values
                                                   :key fkey}))
          (try
            (let [old-bin (:value (first vals))
                  old (when old-bin
                        (let [bais (ByteArrayInputStream. old-bin)]
                          (second (-deserialize serializer write-handlers bais))))
                  new (if (empty? rkey)
                        (up-fn old)
                        (update-in old rkey up-fn))]
              (let [baos (ByteArrayOutputStream.)]
                (-serialize serializer baos write-handlers [key-vec new])
                (kv/store conn (:name bucket-opts) id (.toByteArray baos)))
              (put! res-ch [(get-in old rkey)
                            (get-in new rkey)]))
            res-ch
            (catch Exception e
              (put! res-ch (ex-info "Could not write key."
                                    {:type :write-error
                                     :key fkey
                                     :exception e}))
              res-ch)
            (finally
              (close! res-ch)))))))
  (-dissoc [this key]
    (go
      (let [id (str (uuid key))]
        (try
          (kv/delete conn (:name bucket-opts) id)
          nil
          (catch Exception e
            (ex-info "Could not delete key."
                     {:type :delete-error
                      :key key
                      :exception e}))))))

  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (let [id (str (uuid key))
          vals (:result (kv/fetch conn (:name bucket-opts) id))]
      (cond (zero? (count vals))
            (go nil)

            (> (count vals) 1)
            (go (ex-info "This key has a conflict." {:type :multiple-values
                                                     :key key}))

            :else
            (go
              (try
                (let [bais (ByteArrayInputStream. (:value (first vals)))]
                  (locked-cb {:input-stream bais
                              :size (count (first vals))}))
                (catch Exception e
                  (ex-info "Could not read key."
                           {:type :read-error
                            :key key
                            :exception e})))))))

  (-bassoc [this key input]
    (let [id (str (uuid key))]
      (go
        (try
          (kv/store conn (:name bucket-opts) id input)
          nil
          (catch Exception e
            (ex-info "Could not write key."
                     {:type :write-error
                      :key key
                      :exception e})))))))



(defn new-welle-store
  [& {:keys [conn-url bucket-opts serializer read-handlers write-handlers]
      :or {serializer (ser/fressian-serializer)
           read-handlers (atom {})
           write-handlers (atom {})
           conn-url "http://127.0.0.1:8098/riak"
           bucket-opts {:name "konserve"
                        :opts {}}}}]
  (go (try
        (let [conn (wc/connect conn-url)]
          (map->WelleStore {:conn-url conn-url
                            :bucket-opts bucket-opts
                            :conn conn
                            :read-handlers read-handlers
                            :write-handlers write-handlers
                            :serializer serializer
                            :locks (atom {})}))
        (catch Exception e
          e))))




(comment

  (def store (<!! (new-welle-store :conn-url "http://localhost:32780/riak")))

  (require '[konserve.core :as k])

  (<!! (k/get-in store ["foo"]))

  (<!! (k/exists? store "foo"))

  (<!! (k/assoc-in store ["foo"] {:foo 42}))

  (<!! (k/update-in store ["foo" :foo] inc))

  (<!! (k/bget store ["foo"] (fn [arg]
                               (let [baos (ByteArrayOutputStream.)]
                                 (io/copy (:input-stream arg) baos)
                                 (prn "cb" (vec (.toByteArray baos)))))))

  (<!! (k/bassoc store ["foo"] (byte-array [42 42 42 42 42])))




  )


