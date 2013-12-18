(ns atrium.pulse
  (:require [clojure.core.async :refer [chan thread <!! >!!]]
            [zookeeper :as zk]
            [dire.core :as d]))

(defn serialize-edn [x]
  (.getBytes (pr-str x)))

(defn deserialize-edn [x]
  (read-string (String. x "UTF-8")))

(defn zk-addr [host port]
  (str host ":" port))

(defn launch! [{:keys [host port id master-path] :as config}]
  (let [user-ch (chan)
        client (zk/connect (zk-addr host port))]
    (thread
     (loop [elect-ch (chan)]
       (zk/exists client master-path :watcher (fn [x] (>!! elect-ch true)))
       (zk/create client master-path :data (serialize-edn id))
       (<!! elect-ch)
       (zk/create client master-path :data (serialize-edn id))
       (if (= (deserialize-edn (:data (zk/data client master-path))) id)
         (>!! user-ch true)
         (recur (chan)))))
    user-ch))

(defn master [{:keys [host port master-path] :as config}]
  (let [client (zk/connect (zk-addr host port))]
    (deserialize-edn (:data (zk/data client master-path)))))

