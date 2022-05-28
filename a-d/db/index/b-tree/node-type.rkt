#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*                        B-Tree Node Type                         *-*-
;-*-*                                                                 *-*-
;-*-*              Theo D'Hondt and Wolfgang De Meuter                *-*-
;-*-*             1993 - 2007 Programming Technology Lab              *-*-
;-*-*                   Vrije Universiteit Brussel                    *-*-
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

(library
 (node-type)
 (export new node-type? 
         disk key-type key-size leaf-capacity internal-capacity key-sent)
 (import (rnrs base)
         (srfi :9)
         (a-d file constants)
         (prefix (a-d db rcid) rcid:)
         (prefix (a-d disk config) disk:))
 
 (define-record-type node-type
   (make d t s l i)
   node-type?
   (d disk)
   (t key-type)
   (s key-size)
   (l leaf-capacity)
   (i internal-capacity))
 
 (define (key-sent ntyp)
   (sentinel-for (key-type ntyp) (key-size ntyp)))

 (define (new disk key-type key-size)
   (define leaf-node-cpty 
     (div (- disk:block-size disk:block-ptr-size)
          (+ key-size rcid:size)))
   (define internal-node-cpty 
     (div (- disk:block-size disk:block-ptr-size)
          (+ key-size disk:block-ptr-size)))
   (if (or (< leaf-node-cpty 2)
           (< internal-node-cpty 2))
       (error "key too large (new)" key-size)
       (make disk key-type key-size leaf-node-cpty internal-node-cpty))))