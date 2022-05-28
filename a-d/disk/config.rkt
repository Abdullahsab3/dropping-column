#lang r6rs

;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*                                                                 *-*-
;-*-*                   Disks (Configuration File)                    *-*-
;-*-*                                                                 *-*-
;-*-*                       Wolfgang De Meuter                        *-*-
;-*-*                   2010  Software Languages Lab                  *-*-
;-*-*                    Vrije Universiteit Brussel                   *-*-
;-*-*                                                                 *-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
;-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

(library
 (disk)
 (export block? disk position 
         new mount unmount name 
         block-size disk-size
         block-idx-size block-ptr-size
         real32 real64
         integer-bytes natural-bytes
         read-block write-block!
         decode-byte encode-byte! 
         decode-fixed-natural encode-fixed-natural!
         decode-arbitrary-integer encode-arbitrary-integer! 
         decode-real encode-real! 
         decode-string encode-string!
         decode-bytes encode-bytes!)
 (import (rnrs base)
         (rnrs control)
         (rnrs mutable-pairs)
         (a-d disk disk)
         ;(a-d disk cached-disk)
         ;(Hoofdstuk14 Oplossingen cached-disk-info)
         ;(Hoofdstuk14 Oplossingen cached-disk-wbwa-info)
         )
 )