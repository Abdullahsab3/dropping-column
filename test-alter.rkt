#lang r6rs

(import (prefix (a-d disk disk) disk:)
        (prefix (a-d disk file-system) fs:)
        (prefix (a-d db database) db:)
        (prefix (a-d db table fixed-size-slots schema) scma:)
        (prefix (a-d db table fixed-size-slots table) tbl:)
        (a-d file constants)
        (rnrs base)
        (rnrs control)
        (rnrs io simple)
        (only (racket base) format))
;Create a new disk and format it
(define my-computer (disk:new "HardDisk"))

(fs:format! my-computer)

(display "TEST WITH THE PROVIDED EXAMPLE\n")

(define grondstofprijzen (db:new my-computer "grondstofprijzen"))

(define :jaar:            0)
(define :maand:           1)
(define :Benz-95-E5:      2)
(define :Benz-95-E10:     3)
(define :Dies-B7:         4)
(define :Dies-B10:        5)
(define :Dies-XTL:        6)

(define olieprijs-schema '((natural 2) ;; jaar
                           (natural 2) ;; maand
                           (decimal)   ;; Benz-95-E5 
                           (decimal)   ;; Benz-95-E10
                           (decimal)   ;; Dies-B7
                           (decimal)   ;; Dies-B10
                           (decimal))) ;; Dies-XTL

(define olieprijs (db:create-table grondstofprijzen "olieprijs" olieprijs-schema))
(db:create-index! grondstofprijzen olieprijs "DIES-B7-IDX"   :Dies-B7:)


(display "FILLING UP TABLE") (newline)
(display "----------------------------------------\n")
(db:insert-into-table! grondstofprijzen olieprijs (list 2021 10 1.7451   1.6991   1.715   1.7002  2.9368))
(db:insert-into-table! grondstofprijzen olieprijs (list 2021 11 1.8039 1.7628 1.7594 1.7592 3.003))
(db:insert-into-table! grondstofprijzen olieprijs (list 2021 12 1.7183 1.6714 1.6982 1.6923 2.9645))
(db:insert-into-table! grondstofprijzen olieprijs (list 2022 1 1.7751 1.7197 1.7664 1.7505 3.3862))
(db:insert-into-table! grondstofprijzen olieprijs (list 2022 2 1.8549 1.7933 1.8424 1.8186 3.4738))
(db:insert-into-table! grondstofprijzen olieprijs (list 2022 3 1.9253 1.8694 2.0586 2.0361 3.6533))
(display "TABLE SCHEMA BEFORE DROPPING Dies-B7\n\n")
(scma:print-schema (tbl:schema olieprijs))
(tbl:print olieprijs)
(newline)
(display "DROPPING THE Dies-B7 ATTRIBUTE") (newline)
(db:alter-table-drop-column! grondstofprijzen olieprijs :Dies-B7:)
(display "TABLE SCHEMA AFTER DROPPING Dies-B7\n")
(scma:print-schema (tbl:schema olieprijs))
(tbl:print olieprijs)

(display "----------------------------------------\n")
(display "Database from fivethirtyeight\n\n")

;; bron: https://github.com/fivethirtyeight/data/blob/master/alcohol-consumption/drinks.csv
;; de dataset wordt een beetje aangepast.

(define (insert-data-from-file! file dbse table max-tuples)
  (let ((file-handle (open-input-file file)))
    (define (get-tuple)
      (let loop ((idx 0))
        (cond ((= idx (scma:nr-of-attributes (tbl:schema table))) ())
              
              (else (let ((input (read file-handle)))
                      (if (eof-object? input)
                          ()
                          (cons (if (symbol? input) (symbol->string input) input) (loop (+ idx 1)))))))))
    (let loop ((idx 0))
      (unless (= idx max-tuples)
        (db:insert-into-table! dbse table (get-tuple))
        (loop (+ idx 1))))))

(define fivethirtyeight (db:new my-computer "fivethirtyeight"))

(define :country: 0)
(define :beer_servings 1)
(define :spirit_servings: 2)
(define :wine_servings: 3)
(define :total_litres_of_pure_alcohol: 4)

(define drinks-schema '((string 20) ;; country
                        (natural 2) ;; beer_servings
                        (natural 2) ;; spirit_servings
                        (natural 2) ;; wine_servings
                        (decimal))) ;; total_litres_of_pure_alcohol

(define drinks (db:create-table fivethirtyeight "drinks" drinks-schema))
(db:create-index! fivethirtyeight drinks "name-iDX" :country:)
(display "INSERTING DATA INTO DRINKS TABLE\n")
(insert-data-from-file! "drinks.csv" fivethirtyeight drinks 170)
(tbl:print drinks)


(db:alter-table-drop-column! fivethirtyeight drinks :country:)
(display "DROPPING THE COUNTRY COLUMN\n")
(tbl:print drinks)
(set! :beer_servings (- :beer_servings 1))
(set! :spirit_servings: (- :spirit_servings: 1))
(set! :wine_servings: (- :wine_servings: 1))
(set! :total_litres_of_pure_alcohol: (- :total_litres_of_pure_alcohol: 1))



(db:alter-table-drop-column! fivethirtyeight drinks :wine_servings:)
(display "DROPPING THE wine_servings COLUMN\n")
(tbl:print drinks)
(set! :total_litres_of_pure_alcohol: (- :total_litres_of_pure_alcohol: 1))

(display "DROPPING THE total_litres_of_pure_alcohol COLUMN\n")
(db:alter-table-drop-column! fivethirtyeight drinks :total_litres_of_pure_alcohol:)
(tbl:print drinks)

(display "DROPPING THE spirit_servings COLUMN\n")
(db:alter-table-drop-column! fivethirtyeight drinks :spirit_servings:)
(tbl:print drinks)

(display "DROPPING THE beer_servings COLUMN\n")
(db:alter-table-drop-column! fivethirtyeight drinks :beer_servings)
(tbl:print drinks)

                                