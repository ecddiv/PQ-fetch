(ns PQ-fetch.PQ-fetch
  (:require [tablecloth.api :as tc]
            [clj-http.client :as client]
            [cheshire.core :as json]
            [java-time.api :as jt]
            [clojure.java.io :as io]))

;; Steps:
;; 1. Get PQ data
;; 2. Use list of unique member names from PQ data to get member party data
;; 3. Add in the party data to PQ data

;; Step 1 - PQ Data
;; Due to the limitation of the api (9999 results max), PQs are fetched by month

(defn make-url [date-start date-end]
  (str "https://api.oireachtas.ie/v1/questions"
       "?date_start=" date-start
       "&date_end=" date-end
       "&limit=9999"))

(defn parse-row-data [{:keys [question]}]
  (let [date        (:date question)
        member      (-> question :by :showAs)
        member-code (-> question :by :memberCode)
        house       (-> question :house :showAs)
        ;; q    (:showAs question)
        topic       (-> question :debateSection :showAs)
        type        (:questionType question)
        department  (-> question :to :showAs)]
    {:Date date
     :Member member
     :Member-Code member-code
     :House house
     ;; :Question q
     :Topic topic
     :Type type
     :Department department}))

(defn date-range
  "Given month, returns month start and month end"
  [year month-number]
  (let [start (jt/local-date year month-number)
        end (jt/minus
             (jt/plus start (jt/months 1))
             (jt/days 1))]
    (mapv str [start end])))


(defn get-monthly-PQ-data [[month-start month-end]]
  (let [question-resp (client/get (make-url month-start month-end)
                                  {:accept :json})
        question-resp-data (-> question-resp
                               :body
                               (json/parse-string true)
                               :results)]
    (pmap parse-row-data question-resp-data)))

(defn get-annual-PQ-data! [year output-location]
  (let [filename    (str year "_PQs.csv")
        output-file (str output-location "/" filename)]
    (loop [month 1
           data  []]
      (if (> month 12)
        (-> data
            tc/dataset
            (tc/write! output-file))
        (let [dates (date-range year month)]
          (recur
           (inc month)
           (concat data (get-monthly-PQ-data dates))))))))

;; Example usage: get PQ data for a year
(comment
  (get-annual-PQ-data! 2024 "resources/PQ_data"))


(defn join-annual-pq-data [directory]
  (let [files (rest (file-seq (io/file directory)))]
    (apply tc/concat
           (reduce (fn [result file]
                     (conj result (tc/dataset file {:key-fn keyword})))
                   []
                   files))))


;; Step 2 - Party Data

(defn member-code->uri [code]
  (str "https://data.oireachtas.ie/ie/oireachtas/member/id/" code))

(defn party-membership-rows [full-name member-code memberships]
  (let [parties (->> memberships
                     (map :membership)
                     (map :parties)
                     (reduce concat))]
    (reduce (fn [result {:keys [party]}]
              (conj result
                    {:member-name full-name
                     :member-code member-code
                     :party-name  (:showAs party)
                     :date-from   (:start (:dateRange party))
                     :date-to     (:end (:dateRange party))}))
            []
            parties)))

(defn get-member-party-rows [member-code]
  (let [resp (client/get
              (str "https://api.oireachtas.ie/v1/members?"
                   "date_start=1900-01-01"
                   "&chamber_id="
                   "&member_id=" (member-code->uri member-code)
                   "&limit=1"))
        member-data (-> resp
                        :body
                        (json/parse-string true)
                        :results
                        first
                        :member)
        member-name (:fullName member-data)
        member-code (:memberCode member-data)]
    (party-membership-rows member-name member-code (:memberships member-data))))

(defn write-member-party-table! [unique-members output-location]
  (let [filename (str (jt/format "YYYYMMddHHmmSS" (jt/local-date-time)) "_parties.csv")]
    (->
     (reduce concat
             (mapv get-member-party-rows unique-members))
     (tc/dataset)
     (tc/write! (str output-location "/" filename)))))

(defn make-member-parties-list! [PQ-data-location output-location]
  (let [unique-members (-> PQ-data-location
                           join-annual-pq-data
                           :Member-Code
                           distinct)]
    (write-member-party-table! unique-members output-location)))

;; Example usage
(comment
  (make-member-parties-list! "resources/PQ_data" "resources/Party_data"))

;; Step 3 - Add party data to PQ data from Step 1

(defn in-date-range? [date-from target date-to]
  (if date-to
    (jt/<= (jt/local-date date-from)
           (jt/local-date target)
           (jt/local-date date-to))
    (jt/>= (jt/local-date target)
           (jt/local-date date-from))))


(defn lookup-party [party-data member-code question-date]
  (-> party-data
      (tc/select-rows #(= member-code (:member-code %)))
      (tc/select-rows #(in-date-range?
                        (:date-from %)
                        question-date
                        (:date-to %)))
      :party-name
      first))

;; See end of file for more details on this cleaning step
(def member-errors-list
  #{"Gabrielle-McFadden.D.2014-05-23"
    "Paul-Murphy.D.2014-10-10"
    "Ruth-Coppinger.D.2014-05-23"
    "Michael-Fitzmaurice.D.2014-10-10"
    "Helen-McEntee.D.2013-03-27"
    "Bobby-Aylward.D.2007-06-14"
    "Kathleen-Funchion.D.2016-10-03"
    "Brendan-Ryan.S.1981-08-10"
    "Brendan-Ryan.S.1982-05-13"})

(defn lookup-party-wrapper [party-data member-code question-date]
  (if-not (member-errors-list member-code)
    (lookup-party party-data member-code question-date)
    (case member-code
      "Gabrielle-McFadden.D.2014-05-23"  "Fine Gael"
      "Paul-Murphy.D.2014-10-10"         "Anti-Austerity Alliance - People Before Profit"
      "Ruth-Coppinger.D.2014-05-23"      "Anti-Austerity Alliance - People Before Profit"
      "Michael-Fitzmaurice.D.2014-10-10" "Independent"
      "Helen-McEntee.D.2013-03-27"       "Fine Gael"
      "Bobby-Aylward.D.2007-06-14"       "Fianna Fáil"
      "Kathleen-Funchion.D.2016-10-03"   "Sinn Féin"
      "Brendan-Ryan.S.1981-08-10"        "Labour"
      "Brendan-Ryan.S.1982-05-13"        "Labour")))

(defn add-party-affiliation [dataset party-data]
  (-> dataset
      (tc/map-columns :Party [:Member-Code :Date]
                      #(lookup-party-wrapper party-data %1 (str %2)))))


(defn create-output-dataset [PQ-data-location Party-data-file output-location]
  (let [output-filename (str (jt/format "YYYYMMddHHmm" (jt/local-date-time)) "_PQs.csv")
        pq-ds           (-> (join-annual-pq-data PQ-data-location)
                            (tc/drop-missing :Member-Code))
        party-ds        (tc/dataset Party-data-file {:key-fn keyword})
        joined-ds       (add-party-affiliation pq-ds party-ds)]
    (tc/write! joined-ds (str output-location "/" output-filename))))



;; Notes:

;; Missing 'Party' values
;; After a first pass, it appeared there were 2,468 missing 'Party' values out of 636,153 rows (0.39%)
;; On further inspection, this appeared to be due to an error with the Oireachtas API for the following members.

;; ("Gabrielle-McFadden.D.2014-05-23"
;; "Paul-Murphy.D.2014-10-10"
;; "Ruth-Coppinger.D.2014-05-23"
;; "Michael-Fitzmaurice.D.2014-10-10"
;; "Helen-McEntee.D.2013-03-27"
;; "Bobby-Aylward.D.2007-06-14"
;; "Kathleen-Funchion.D.2016-10-03"
;; "Brendan-Ryan.S.1981-08-10"
;; "Brendan-Ryan.S.1982-05-13")

;; It looks like in these cases, the 'start' date tended to be wrong, and higher than the 'end' date
;; These were all the members with a higher 'start' date than 'end' date:

;; ("Michael-Ring.D.1994-06-09"
;; "Gabrielle-McFadden.D.2014-05-23"
;; "Paul-Murphy.D.2014-10-10"
;; "Ruth-Coppinger.D.2014-05-23"
;; "Michael-Fitzmaurice.D.2014-10-10"
;; "Shane-PN-Ross.S.1981-10-08"
;; "Seamus-Healy.D.2000-06-22"
;; "Helen-McEntee.D.2013-03-27"
;; "Bobby-Aylward.D.2007-06-14"
;; "Richard-Bruton.S.1981-10-08"
;; "Simon-Coveney.D.1998-10-23"
;; "Enda-Kenny.D.1975-11-12")
;;
;; You can see these errors if you go to the Oireachtas API and search for these member ids under the '/members' api:
;; e.g., "https://data.oireachtas.ie/ie/oireachtas/member/id/Paul-Murphy.D.2014-10-10"
;;
;; or, with curl:
;; curl -X GET "https://api.oireachtas.ie/v1/members?date_start=1900-01-01&chamber_id=&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FPaul-Murphy.D.2014-10-10&date_end=2099-01-01&limit=50" -H  "accept: application/json"
;;

;; Separately, there also seemed to be an issue with the PQ api, where 'Brendan Ryan' the TD was being confused with another
;; 'Brendan Ryan' (a Seanad member)
;; Brendan Ryan (Labour TD)
;;  - https://en.wikipedia.org/wiki/Brendan_Ryan_(Dublin_politician)
;;  - https://www.oireachtas.ie/en/members/member/Brendan-Ryan.S.2007-07-23/
;; Bendan Ryan (Independant Senator)
;;  - https://en.wikipedia.org/wiki/Brendan_Ryan_(Cork_politician)
;;  - https://www.oireachtas.ie/en/members/member/Brendan-Ryan.S.1981-08-10/
;;
;; You can see this error if you go to the Oirechtas API website and search for PQs with the member id:
;; "https://data.oireachtas.ie/ie/oireachtas/member/id/Brendan-Ryan.S.1982-05-13"
;;
;; or, with curl:
;; curl -X GET "https://api.oireachtas.ie/v1/questions?date_start=1900-01-01&date_end=2099-01-01&limit=50&qtype=oral,written&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FBrendan-Ryan.S.1982-05-13" -H  "accept: application/json"

;; Or, you can also see the error in action if you go to one of these PQs with missing Party information. For example:
;; "https://www.oireachtas.ie/en/debates/question/2012-06-28/90/#pq-answers-86_87_88_89_90"
;; Hovering over the link for "Brendan Ryan" will show a url for Brendan Ryan (Senator), and clicking on the link
;; will return a 404 error.
