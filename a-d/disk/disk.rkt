#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*                          Disk & Block                           *-*-
;-*-*                                                                 *-*-
;-*-*                       Wolfgang De Meuter                        *-*-
;-*-*                 2010 Programming Technology Lab                 *-*-
;-*-*                   Vrije Universiteit Brussel                    *-*-
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

(library
 (disk)
 (export block? disk position 
         new name mount unmount disk-size
         block-size block-ptr-size 
         block-idx-size 
         real32 real64
         read-block write-block!
         decode-byte encode-byte! 
         decode-fixed-natural encode-fixed-natural!
         decode-arbitrary-integer encode-arbitrary-integer! 
         integer-bytes natural-bytes
         decode-real encode-real! 
         decode-string encode-string!
         decode-bytes encode-bytes!)
 (import (rnrs base)
         (rnrs io ports)
         (rnrs io simple)
         (rnrs control)
         (rnrs bytevectors)
         (srfi :9))
 
 (define real32 4)
 (define real64 8)
 
 (define (integer-bytes nmbr)
   (exact (ceiling (log (max (abs (+ (* 2 nmbr) 1)) 2) 256))))
 
 (define (natural-bytes nmbr)
   (exact (ceiling (log (max (+ nmbr 1) 2) 256))))
 
 (define block-size 100)         ; nr of bytes in a block
 (define disk-size  3000)       ; nr of blocks
 (define block-ptr-size (natural-bytes disk-size))  ; nr of bytes used to encode a block nr (i.e. a pointer on disk)
 (define block-idx-size (natural-bytes block-size)) ; nr of bytes used to encode an offset within a block
   
 (define str-end-byte 0)

 (define-record-type disk
   (make-disk n)
   disk?
   (n name))
 
 (define (new name) 
   (define port (open-file-output-port name 
                                       (file-options no-truncate no-fail)
                                       (buffer-mode block)))
   (define zeroes (make-bytevector block-size 0))
   (let low-level-format
     ((block-nr 0))
     (put-bytevector port zeroes)
     (if (< (+ 1 block-nr) disk-size)
         (low-level-format (+ 1 block-nr))))
   (close-port port)
   (make-disk name))
 
 (define (mount name)
   (make-disk name))
 
 (define (unmount dsk)
   ()) ; default is to do nothing ; cached version does more
  
 (define-record-type block
   (make-block d p b)
   block?
   (d disk)
   (p position)
   (b bytes))

 (define (write-block! blck) 
   (define bptr (position blck))
   (define data-byts (bytes blck))
   (define port (open-file-output-port (name (disk blck)) 
                                       (file-options no-truncate no-fail)))
   (set-port-position! port (* bptr block-size))
   (put-bytevector port data-byts)
   (close-port port))
 
 (define (read-block dsk bptr)
   (define port (open-file-input-port (name dsk) (file-options no-fail)))
   (set-port-position! port (* bptr block-size))
   (let ((byts (get-bytevector-n port block-size)))
     (close-port port)
     (make-block dsk bptr byts)))
 
 (define (encode-byte! blck offs byte)
   (bytevector-u8-set! (bytes blck) offs byte))
 
 (define (decode-byte blck offs)
   (bytevector-u8-ref (bytes blck) offs))
 
 (define (encode-fixed-natural! blck offs size nmbr)
   (bytevector-uint-set! (bytes blck) offs nmbr 'big size))
 
 (define (decode-fixed-natural blck offs size)
   (bytevector-uint-ref (bytes blck) offs 'big size))
 
 (define (encode-arbitrary-integer! blck offs nmbr)
   (define size (integer-bytes nmbr))
   (encode-byte! blck offs size)
   (bytevector-sint-set! (bytes blck) (+ offs 1) nmbr 'big size)
   (+ size 1))
  
 (define (decode-arbitrary-integer blck offs)
   (define size (decode-byte blck offs))
   (define nmbr (bytevector-sint-ref (bytes blck) (+ offs 1) 'big size))
   (cons nmbr (+ offs size 1)))
 
 (define (encode-real! blck offs size nmbr)
   (cond ((= size real64)
          (bytevector-ieee-double-set! (bytes blck) offs nmbr 'big)
          size)
         ((= size real32)
          (bytevector-ieee-single-set! (bytes blck) offs nmbr 'big)
           size)
         (else
          (error "illegal real size" size))))
          
 (define (decode-real blck offs size)
   (cond ((= size real64)
          (bytevector-ieee-double-ref (bytes blck) offs 'big))
         ((= size real32)
          (bytevector-ieee-single-ref (bytes blck) offs 'big))
         (else
          (error "illegal real size" size))))

           
 (define (encode-string! blck offs size strg)
   (set! strg (string->utf8 strg))
   (do ((indx 0 (+ indx 1)))
     ((= indx size))
     (let ((byte (if (< indx (bytevector-length strg))
                     (bytevector-u8-ref strg indx)
                     str-end-byte)))
       (encode-byte! blck (+ offs indx) byte))))
 
 (define (decode-string blck offs size)
   (let ((bytevector-u8-set! 
          (lambda (bv indx val)
            (bytevector-u8-set! bv indx val)
            bv)))
     (utf8->string
      (let loop
        ((indx 0))
        (if (< indx size)
            (let ((byte (decode-byte blck (+ offs indx))))
              (if (eq? byte str-end-byte)
                  (make-bytevector indx 0)
                  (bytevector-u8-set! (loop (+ indx 1)) indx byte)))
            (make-bytevector indx 0))))))
 
  (define (decode-bytes blck byts blck-offs byts-offs size)
    (do ((indx 0 (+ indx 1)))
      ((= indx size))
      (bytevector-u8-set! byts
                          (+ byts-offs indx) 
                          (decode-byte blck (+ blck-offs indx)))))
  
  (define (encode-bytes! blck byts blck-offs byts-offs size)
    (do ((indx 0 (+ indx 1)))
      ((= indx size))
      (encode-byte! blck
                    (+ blck-offs indx)
                    (bytevector-u8-ref byts (+ byts-offs indx)))))
  
  )
