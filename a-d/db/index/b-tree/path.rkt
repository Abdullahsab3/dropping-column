#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*                               Paths                             *-*-
;-*-*                                                                 *-*-
;-*-*                        Wolfgang De Meuter                       *-*-
;-*-*                   2012 Software Languages Lab                   *-*-
;-*-*                   Vrije Universiteit Brussel                    *-*-
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

(library
 (path)
 (export new empty? pop! push! node slot clear!)
 (import (rnrs base)
         (prefix (a-d stack linked) stck:)
         (rnrs control)
         (rnrs mutable-pairs)
         (prefix (a-d db index b-tree node) node:))
 
 (define new    stck:new)

 (define empty? stck:empty?)
 
 (define pop!  stck:pop!)

 (define (push! stck node slot)
   (stck:push! stck (cons node slot)))
 
 (define (node stck) 
   (car (stck:top stck)))
 
 (define (slot stck)
   (cdr (stck:top stck)))
 
 (define (clear! stck)
   (let loop
     ()
     (when (not (empty? stck))
       (pop! stck)
       (loop))))
 
; (define (print path)
;   (display "STACK = <---] ")
;   (if (not (null? (cdr path)))
;       (let loop
;         ((s (cdr path)))
;         (display "< ")
;         (display (node:position (car (car s))))
;         (display ",")
;         (display (cdr (car s)))
;         (display ">")
;         (when (not (null? (cdr s)))
;           (loop (cdr s)))))
;   (display "]]]")(newline))
 
 )