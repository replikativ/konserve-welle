# konserve-welle

A Riak backend for [konserve](https://github.com/replikativ/konserve) implemented with [welle](http://clojureriak.info/). 

## Usage

Add to your leiningen dependencies:
[![Clojars Project](http://clojars.org/io.replikativ/konserve-welle/latest-version.svg)](http://clojars.org/io.replikativ/konserve-welle)

The purpose of konserve is to have a unified associative key-value interface for
edn datastructures and binary blobs. Use the standard interface functions of konserve.

You can provide welle configuration options to the `new-welle-store`
constructor as an `:bucket-config` argument. We do not require additional settings
beyond the konserve serialization protocol for the store, so you can still
access the store through `welle` directly wherever you need (e.g. for
store deletion).

~~~clojure
  (require '[konserve-welle.core :refer :all]
           '[konserve.core :as k)
  (def welle-store (<!! (new-welle-store))) ;; connects to localhost

  (<!! (k/exists? welle-store  "john"))
  (<!! (k/get-in welle-store ["john"]))
  (<!! (k/assoc-in welle-store ["john"] 42))
  (<!! (k/update-in welle-store ["john"] inc))
  (<!! (k/get-in welle-store ["john"]))
~~~


## License

Copyright © 2016 Christian Weilbach

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

