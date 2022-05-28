#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*                  Record Identification Pointers                 *-*-
;-*-*                                                                 *-*-
;-*-*                       Wolfgang De Meuter                        *-*-
;-*-*                   2010  Software Languages Lab                  *-*-
;-*-*                    Vrije Universiteit Brussel                   *-*-
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

(library
 (file-system)
 (export new bptr slot rcid->fixed fixed->rcid size null null?)
 (import (except (rnrs base) null?)
         (a-d file constants)
         (prefix (a-d disk disk) disk:)
         (prefix (a-d disk file-system) fs:))
 
 (define (new bptr slot)
   (cons bptr slot))
 
 (define bptr car)
 
 (define slot cdr)
 
 (define (rcid->fixed rcid)
   (+ (* (expt 256 disk:block-idx-size) (bptr rcid)) (slot rcid)))

 (define (fixed->rcid num)
   (define radx (expt 256 disk:block-idx-size))
   (new (div num radx) (mod num radx)))
 
 (define size (+ disk:block-ptr-size disk:block-idx-size))
 
 (define null (new fs:null-block 0))
 
 (define (null? rcid)
   (and (fs:null-block? (bptr rcid))
        (= (slot rcid) 0))))
