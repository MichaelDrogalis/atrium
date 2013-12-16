(ns heart-beat.core
  (:require [clojure.core.async :refer [chan thread <!! >!! timeout]]
            [dire.core :refer [with-pre-hook! with-post-hook! with-precondition! with-handler!]]
            [zookeeper :as zk])
  (:import [org.apache.zookeeper KeeperException$BadVersionException]))

(def polling-frequency 1000)

(def pulse-frequency 800)

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(def client (zk/connect "127.0.0.1:2181"))

(def master-node "/master")

(defn pulse-node [id]
  (str "/" id))

(defn create-master-node []
  (zk/create client master-node :persistent? true))

(defn create-pulse-node [id]
  (zk/create client (pulse-node id))
  (let [version (:version (zk/exists client (pulse-node id)))]
    (zk/set-data client (pulse-node id) (serialize-edn {:pulse 0}) version)))

(defn monitor-for-failure
  ([master-id] (monitor-for-failure master-id nil 0))
  ([master-id previous-version missed-beats]
     (if (>= missed-beats 2)
       (throw (ex-info "Master pulse stopped" {:master master-id}))
       (do (<!! (timeout polling-frequency))
           (let [payload (zk/data client (pulse-node master-id))
                 version (:version (:stat payload))]
             (if (not= version previous-version)
               (do (println "Saw the heart beat at version" version)
                   (recur master-id version 0))
               (do (println "Saw a missed heart beat at version" version)
                   (recur master-id version (inc missed-beats)))))))))

(defn stand-by [payload ch]
  (let [master-id (:id (deserialize-edn (:data payload)))]
    (monitor-for-failure master-id)))

(defn trigger-rendezvous [e payload ch]
  (>!! ch (:version (:stat payload))))

(with-handler! #'stand-by
  java.lang.NullPointerException
  trigger-rendezvous)

(with-handler! #'stand-by
  clojure.lang.ExceptionInfo
  trigger-rendezvous)

(defn elect-self [id version]
  (let [node (zk/data client master-node)]
    (zk/set-data client master-node (serialize-edn {:id id}) version)
    true))

(with-handler! #'elect-self
  KeeperException$BadVersionException
  (fn [e id version]
    (println id "was beat by another node in becoming the master")
    false))

(with-post-hook! #'elect-self
  (fn [_] (println "Mastery acquired.")))

(defn beat [id master {:keys [data stat] :as pulse-data}]
  (zk/set-data client (pulse-node id) data (:version stat)))

(defn heart-beat-process [id]
  (future
    (while true
      (let [master (zk/data client master-node)
            {:keys [data stat]} (zk/data client (pulse-node id))]
        (beat id master (zk/data client (pulse-node id))))
      (<!! (timeout pulse-frequency)))))

(with-precondition! #'beat
  :still-master
  (fn [id master pulse-data]
    (= (:id (deserialize-edn (:data master))) id)))

(with-handler! #'beat
  {:precondition :still-master}
  (fn [e id _ _] (println id "is not the master anymore. Dying")))

(with-pre-hook! #'stand-by
  (fn [payload ch] (println "On stand-by")))

(with-pre-hook! #'elect-self
  (fn [id version] (println id "is attempting to become the master")))

(with-pre-hook! #'beat
  (fn [id _ _] (println id "heart beats")))

(defn boot! [id]
  (create-master-node)
  (create-pulse-node id)
  (loop [rendezvous (chan)]
    (let [payload (zk/data client master-node)]
      (future (stand-by payload rendezvous))
      (let [version (<!! rendezvous)]
        (if (elect-self id version)
          (heart-beat-process id)
          (recur (chan)))))))

(defn demo []
  (let [a (str (java.util.UUID/randomUUID))]
    (boot! a)))

