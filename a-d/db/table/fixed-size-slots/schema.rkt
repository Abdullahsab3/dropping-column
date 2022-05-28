#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*                      Relational Table Schema                    *-*-
;-*-*                                                                 *-*-
;-*-*                        Wolfgang De Meuter                       *-*-
;-*-*                   2010  Software Languages Lab                  *-*-
;-*-*                    Vrije Universiteit Brussel                   *-*-`
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-


(library
 (schema)
 (export new open delete!
         nr-of-occupancy-bytes nr-of-attributes record-size type size capacity
         fixed-header-size disk position
         copy-schema-w/o-attribute
         block print-schema)
 (import (prefix (a-d disk disk) disk:)
         (prefix (a-d disk file-system) fs:)
         (prefix (a-d db rcid) rcid:)
         (a-d file constants)
         (rnrs base)
         (srfi :9)
         (rnrs lists)
         (rnrs control)
         (rnrs arithmetic bitwise)
         (rnrs io simple)
         (rnrs mutable-pairs)
         (rnrs bytevectors))
 
 (define schema-nr-size 2)   ; numbers of the schema as in (natural 7) encoded with 2 bytes
 (define fixed-header-size (* 2 disk:block-ptr-size))
 ; next, previous; variable # of occupancy bits to be added dynamically
 
 (define-record-type schema
   (make b)
   schema?
   (b block))

 (define cap-offset         0)
 (define rsz-offset         (+ cap-offset schema-nr-size))
 (define schema-size-offset (+ rsz-offset schema-nr-size))
 (define schema-info-offset (+ schema-size-offset 1)) ; size is only one byte
 
 (define (capacity! scma nmbr)
   (define blck (block scma))
   (disk:encode-fixed-natural! blck cap-offset schema-nr-size nmbr))
 (define (capacity scma)
   (define blck (block scma))
   (disk:decode-fixed-natural blck cap-offset schema-nr-size))
 
 (define (record-size! scma nmbr)
   (define blck (block scma))
   (disk:encode-fixed-natural! blck rsz-offset schema-nr-size nmbr))
 (define (record-size scma)
   (define blck (block scma))
   (disk:decode-fixed-natural blck rsz-offset schema-nr-size))
 
 (define (nr-of-attributes scma)
   (define blck (block scma))
   (disk:decode-byte blck schema-size-offset))
 (define (nr-of-attributes! scma nmbr)
   (define blck (block scma))
   (disk:encode-byte! blck schema-size-offset nmbr))
 
 (define (type! scma indx type)
   (define blck (block scma))
   (disk:encode-byte! blck (+ schema-info-offset indx) type))
 (define (type scma indx)
   (define blck (block scma))
   (disk:decode-byte blck (+ schema-info-offset indx)))

 ;; toegevoegd-------------------------------------------------------------------------
 ;; zal een kopie van het schema maken, zowel in disk als in central memory
 (define (copy-schema old-scma)
   (define dsk (disk old-scma))
   (define new-scmablock (fs:new-block dsk))
   (let ((byte-vector (make-bytevector disk:block-size 0)))
     (disk:decode-bytes (block old-scma) byte-vector 0 0 disk:block-size) ; kopieer alle bytes van file-block naar byte-vector
     (disk:encode-bytes! new-scmablock byte-vector 0 0 disk:block-size)
     (make new-scmablock)))

 ;; zal het type field teruggeven als een symbool
  (define (get-symbol-field-type fild)
   (cond ((eq? fild natural-tag) 'natural)
         ((eq? fild integer-tag) 'integer)
         ((eq? fild decimal-tag) 'decimal)
         ((eq? fild string-tag) 'string)
         (else (error "unsported field type" fild))))
  
 (define (print-schema scma)
   (newline)
   (let loop ((idx 0))
     (cond ((= idx (nr-of-attributes scma)) (newline))
           (else (display "TYPE ")
                 (display (get-symbol-field-type (type scma idx)))
                 (display " ")
                 (display "SIZE: ")
                 (display (size scma idx))
                 (newline)
                 (loop (+ idx 1))))))

 ;; zal een kopie maken van het schema en zorgen dat het meegegeven attribuut verwijderd wordt
 ;; uit die kopie.
 (define (copy-schema-w/o-attribute old-scma attribute)
   (let ((new-scma (copy-schema old-scma)))
     (remove-attribute! new-scma attribute)
     (disk:write-block! (block new-scma))
     new-scma))
                       
 (define (calculate-capacity rsz)
   (div (* (- disk:block-size fixed-header-size) 8) 
        (+ (* rsz 8) 1)))

 ;; zal alle types en sizes na het verwijderde attribuut naar links opschuiven om het gat dicht te maken.
 (define (remove-type/size! scma indx)
   (let loop ((curr (+ indx 1)))
     (cond ((= curr (nr-of-attributes scma)) #t)
           (else (type! scma (- curr 1) (type scma curr))
                 (size! scma (- curr 1) (size scma curr))
                 (loop (+ curr 1))))))

 ;; verwijder het attribuut uit het schema door het gat dicht te maken
 ;; Alle waarden van het schema aanpassen na het verwijderen van het attribuut
 (define (remove-attribute! scma attr-indx)
   (define new-rsz (- (record-size scma) (size scma attr-indx)))
   (define new-cap (calculate-capacity new-rsz))
   (record-size! scma new-rsz)
   (capacity! scma new-cap)
   (remove-type/size! scma attr-indx)
   (nr-of-attributes! scma (- (nr-of-attributes scma) 1)))

 ;---------------------------------------------------------------------------------
         
 (define (size! scma indx nmbr)
   (define blck (block scma))
   (disk:encode-byte! blck (- disk:block-size 1 indx)  nmbr))
 (define (size scma indx)
   (define blck (block scma))
   (disk:decode-byte blck (- disk:block-size 1 indx)))
 
 (define (field-size fild)
   (cond ((eq? (car fild) 'natural) 
          (cadr fild))
         ((eq? (car fild) 'integer)
          (cadr fild))
         ((eq? (car fild) 'decimal)
          disk:real64)
         ((eq? (car fild) 'string)
          (cadr fild))
         (else (error "unsported field type" (car fild)))))
 
 (define (field-type fild)
   (cond ((eq? (car fild) 'natural) 
          natural-tag)
         ((eq? (car fild) 'integer)
          integer-tag)
         ((eq? (car fild) 'decimal)
          decimal-tag)
         ((eq? (car fild) 'string)
          string-tag)
         (else (error "unsported field type" (car fild)))))

 (define (types/sizes! scma atts)
   (let loop
     ((indx 0)
      (atts atts))
     (type! scma indx (field-type (car atts)))
     (size! scma indx (field-size (car atts)))
     (if (not (null? (cdr atts)))
         (loop (+ indx 1) (cdr atts)))))

 (define (sum-of-sizes atts)
   (let loop 
     ((atts atts)
      (sizs 0))
     (if (null? atts)
         sizs
         (loop (cdr atts) (+ (field-size (car atts)) sizs)))))
 
 (define (nr-of-occupancy-bytes schema)
   (define bits (capacity schema))
   (exact (ceiling (/ bits 8))))
 
 (define (disk scma)
   (disk:disk (block scma)))
 
 (define (position scma)
   (disk:position (block scma)))
 
 (define (new dsk atts)
   (define rsz (sum-of-sizes atts))
   ;; toegevoegd
   (define cap (calculate-capacity rsz))
   ;   (define cap (div (* (- disk:block-size fixed-header-size) 8) 
   ;                    (+ (* rsz 8) 1)))
   (if (< cap 1)
       (error "tuples must fit in a block (new schema)" rsz))
   (let* ((blck (fs:new-block dsk))
          (scma (make blck)))
     (capacity!         scma cap)
     (record-size!      scma rsz)
     (nr-of-attributes! scma (length atts))
     (types/sizes!      scma atts)
     (disk:write-block! blck)
     scma))
 
 (define (open dsk bptr)
   (define blck (disk:read-block dsk bptr))
   (define scma (make blck))
   scma)

 (define (delete! scma)
   (fs:delete-block (block scma))) )
