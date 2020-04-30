(ns firestream.analytics
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fire.auth :as fire-auth]
            [fire.core :as fire]))

(set! *warn-on-reflection* 1)
