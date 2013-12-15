(ns heart-beat.core
  (:require [clojure.core.async :refer [chan thread <!! >!! timeout]]
            [dire.core :refer [with-pre-hook! with-post-hook! with-precondition! with-handler!]]
            [zookeeper :as zk]))

(def client (zk/connect "127.0.0.1:2181"))

(def master-node "/master")

(defn pulse-node [id]
  (str "/" id))

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(defn monitor-for-failure
  ([id master-id] (monitor-for-failure id master-id nil 0))
  ([id master-id previous-version missed-beats]
     (if (>= missed-beats 2)
       (throw (ex-info "Master pulse stopped" {:master master-id}))
       (do (<!! (timeout 1000))
           (let [payload (zk/data client (pulse-node master-id))
                 version (:version (:stat payload))]
             (if (not= version previous-version)
               (do (println id "saw the heart beat at version" version)
                   (recur id master-id version 0))
               (do (println id "saw a missed heart beat at version" version)
                   (recur id master-id version (inc missed-beats)))))))))

(defn stand-by [id ch]
  (let [payload (zk/data client master-node)
        master-id (:id (deserialize-edn (:data payload)))]
    (monitor-for-failure id master-id)))

(defn trigger-masterless-rendezvous [e id ch]
  (>!! ch false))

(defn trigger-master-rendezvous [e id ch]
  (>!! ch (:master (ex-data e))))

(defn create-master-place []
  (zk/create client master-node))

(defn create-heart-beat-place [id]
  (zk/create client (pulse-node id))
  (let [version (:version (zk/exists client (pulse-node id)))]
    (zk/set-data client (pulse-node id) (serialize-edn {:pulse 0}) version)))

(defn elect-self [id prev-master]
  (let [node (zk/data client master-node)]
    (if (or (nil? (:data node))
            (= (:id (deserialize-edn (:data node))) prev-master))
      (do (zk/set-data client master-node (serialize-edn {:id id}) (:version (:stat node)))
          (println id "has become the master")
          true)
      (do (println id "was too late. Back to standby")
          false))))

(defn beat [id master {:keys [data stat] :as pulse-data}]
  (zk/set-data client (pulse-node id) data (:version stat)))

(defn heart-beat-process [id]
  (future
    (while true
      (let [master (zk/data client master-node)
            {:keys [data stat]} (zk/data client (pulse-node id))]
        (beat id master (zk/data client (pulse-node id))))
      (<!! (timeout 800)))))

(with-precondition! #'beat
  :still-master
  (fn [id master pulse-data]
    (= (:id (deserialize-edn (:data master))) id)))

(with-handler! #'stand-by
  java.lang.NullPointerException
  trigger-masterless-rendezvous)

(with-handler! #'stand-by
  clojure.lang.ExceptionInfo
  trigger-master-rendezvous)

(with-handler! #'elect-self
  java.lang.NullPointerException
  (fn [& _] false))

(with-handler! #'beat
  {:precondition :still-master}
  (fn [id & _] (println id "is not the master anymore. Dying")))

(with-pre-hook! #'stand-by
  (fn [id ch] (println id "is on stand-by")))

(with-pre-hook! #'trigger-masterless-rendezvous
  (fn [e id ch] (println id "found master with no heart beat")))

(with-pre-hook! #'elect-self
  (fn [id previous-master] (println id "is attempting to become the master")))

(with-pre-hook! #'beat
  (fn [id & _] (println id "heart beats")))

(defn boot! [id]
  (create-master-place)
  (create-heart-beat-place id)
  (loop [rendezvous (chan)]
    (future (stand-by id rendezvous))
    (let [prev-master (<!! rendezvous)]
      (if (elect-self id prev-master)
        (heart-beat-process id)
        (recur (chan))))))

(def a (str (java.util.UUID/randomUUID)))

