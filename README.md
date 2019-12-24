# firestream

Kafkaesque streams built on firebase

## Status
Design and prototyping. Not suitable for any kind of usuage. 

## Design 
`firestream` is designed to be a minimal hassle, kafakaesque streams for pico-scale applications or MVPs. Once your application is no longer pico-scale using `firestream` is a great way to ensure bad things happen. `firestream` is aimed to give your application (or business) the runway grow enough to be able to absorb the operational cost of `kafka`. 

### Schema 
At both rest and in transit all messages are stored as stringified `json` objects. `firestream` itself works in `clojure` maps. 

### Topics and partitions
Partitions are not provided for in `firestream`. If you really really really need partitions, it's probably time to switch to `kafka`.

### Producers and consumers
Like `kafka`, `firestream` allows for multiple consumer and producers. It however does not allow for consumer groups. If you really really really need consumer groups, it's probably time to switch to `kafka`.

### Brokers and clusters
There is only one broker, that broker is your firebase instance. There is only cluster

### Interface
The design of `firestream`'s interface is inspired by [pyr's](https://github.com/pyr) somewhat opinionated client library for `kafka` [kinsky](https://github.com/pyr/kinsky).

## Installation

You can grab `firestream` from clojars: [alekcz/charmander "0.7.1"].

### Connecting to firebase

1. Get the `json` file containing your creditials by following the instruction here [https://firebase.google.com/docs/admin/setup](https://firebase.google.com/docs/admin/setup)  

2. Set the GOOGLE_CLOUD_PROJECT environment to the id of your project

3. Set the FIREBASE_CONFIG environment variable to the contents of your `json` key file. (Sometimes it may be necessary to wrap the key contents in single quotes to escape all the special characters within it. e.g. FIREBASE_CONFIG='`contents-of-json`')

4. You're now good to go.

## Usage

Chill, it's still early days. 


## Metrics

There are no metrics for the moment, but hopefully someday we'll get to the same level as [operatr.io](https://operatr.io/). 

## What next

When you outgrow `firestream` and are ready for `kafka`, hit up the awesome folks at [troywest.com](https://troywest.com/) to get you started.  

## License

Copyright © 2019 Alexander Oloo

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
