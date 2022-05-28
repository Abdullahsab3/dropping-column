#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*           Scheme Tools that should have been included           *-*-
;-*-*                                                                 *-*-
;-*-*                       Wolfgang De Meuter                        *-*-
;-*-*                   2009 Software Languages Lab                   *-*-
;-*-*                    Vrije Universiteit Brussel                   *-*-
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

(library
 (scheme-tools)
 (export vector-map! vector-for-each+ bytevector-u8-map! random-inbetween random-integer make-2D-vector ij! ij?
         current-time time<?)
 (import (rnrs base)
         (srfi :27)
         (srfi :19)
         (rnrs bytevectors))
 
 (define (random-inbetween l r)
   (+ l (random-integer (+ (- r l) 1))))
 
 (define (vector-map! v f)
   (define last (- (vector-length v) 1))
   (let loop
     ((i 0))
     (vector-set! v i (f i (vector-ref v i)))
     (if (< i last)
       (loop (+ i 1))))
   v)
 
 (define (vector-for-each+ v f)
   (define last (- (vector-length v) 1))
   (define res (make-vector (vector-length v) '()))
   (let loop 
     ((i 0))
     (vector-set! res i (f i (vector-ref v i)))
     (if (< i last)
       (loop (+ i 1))))
   res)
 
 (define (bytevector-u8-map! v f)
   (define last (- (bytevector-length v) 1))
   (let loop
     ((i 0))
     (bytevector-u8-set! v i (f i (bytevector-u8-ref v i)))
     (if (< i last)
       (loop (+ i 1))))
   v)
 
 (define (make-2D-vector n m proc)
   (define res (make-vector n '()))
   (vector-map! 
    res 
    (lambda (i el) 
      (define row (make-vector m '()))
      row))
   (vector-map!
    res
    (lambda (i row)
      (vector-map!
       row
       (lambda (j el)
         (proc i j)))
      row)))
 
 (define (ij? v i j)
   (vector-ref (vector-ref v i) j))
 (define (ij! v i j a)
   (vector-set! (vector-ref v i) j a))
 
 
 )