# firestream

Kafkaesque streams built on firebase

![Clojure CI](https://github.com/alekcz/firestream/workflows/Clojure%20CI/badge.svg) [![codecov](https://codecov.io/gh/alekcz/firestream/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/firestream) 

[![Clojars Project](https://img.shields.io/clojars/v/alekcz/firestream.svg)](https://clojars.org/alekcz/firestream)

## Design 
`firestream` is designed to provide kafakaesque streams with minimal hassle for micro-scale applications or MVPs. Once your application is no longer micro-scale using `firestream` is a great way to ensure bad things happen. `firestream` is aimed to give your application (or business) the runway grow until it can absorb the operational cost of `kafka`. 

### Schema 
At both rest and in transit all messages are stored as stringified `json` objects. `firestream` itself works in `clojure` maps. 

### Topics and partitions
Partitions are not provided for in `firestream`. If you really really really need partitions, it's probably time to switch to `kafka`.

### Producers and consumers
Like `kafka`, `firestream` allows for multiple consumer and producers. It however only allows for consumer groups with one consumer. If you really really really need more than one consume in a consumer group, it's probably time to switch to `kafka`.

### Brokers and clusters
There is only one broker, that broker is your firebase instance. There is only one cluster

### Interface
The design of `firestream`'s interface is inspired by [pyr's](https://github.com/pyr) somewhat opinionated client library for `kafka` [kinsky](https://github.com/pyr/kinsky).

## Limits
The theoretical limits* of `firestream` (i.e. running it on the biggest machine you can find) are derived by using an 8th of the limits of firebase. For pico-scale applications or MVPs it's unlikely you'll hit the limits of firebase or `firestream`. Here they are anyway:

- Individual producer throughput: ~2k per second (messages)
- Maximum message size: 1MB (afer `pr-str`)
- Maximmum data transfer per read: 200MB
- Maximum number of messages per topic: 9 million

*It's quite likely that you can get more perf than the above, but better safe than sorry.

[Firebase usage limits in case they change](https://firebase.google.com/docs/database/usage/limits)

## Installation

You can grab `firestream` from clojars:    
`[alekcz/firestream "2.0.1"]`


### Setting up firebase

1. You need to create a project on [firebase](https://firebase.google.com/) to get started. So do that first.
2. Once you've created your project setup a Realtime Database.
3. We don't want any frontends or non-admin apps to access our database, as this database will be at the core of our stream. So we need to deny all non-admin access using the [firebase securtiy rules](https://firebase.google.com/docs/database/security/quickstart). You can use the rules below.
```javascript
{
  "rules": {
    ".read": false, //block all non-admin reads
    ".write": false //block all non-admin writes
  }
}
```

### Connecting your app to firebase

1. Get the `json` file containing your creditials by following the instruction here [https://firebase.google.com/docs/admin/setup](https://firebase.google.com/docs/admin/setup)  

2. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the contents of your `json` key file. Using [google-credentials](https://github.com/alekcz/google-credentials) firestream will pull the credentials from the specified environment variable: `GOOGLE_APPLICATION_CREDENTIALS`. (Sometimes it may be necessary to remove all the line breaks and wrap the key contents in single quotes to escape all the special characters within it. e.g. GOOGLE_APPLICATION_CREDENTIALS='`contents-of-json`')

3. You're now good to go.

## Usage

### Example
```clojure
(require '[firestream.core :as f])

(let [c (f/producer {:env "GOOGLE_APPLICATION_CREDENTIALS"})]
      (f/send! p "emailqueue" :k0 {:name "Alexander the Great" :email "alex@macedon.gr"})   

(let [c (f/consumer {:env "GOOGLE_APPLICATION_CREDENTIALS" :group.id "kafkaesque"})]
      (f/subscribe! c "emailqueue")
      (f/poll! c 100)
```

### API

The `firestream` API has 5 functions. 
- `producer`: Create a producer
- `send!`: Send new message to topic
- `consumer`: Create a consumer
- `subscribe!`: Subscribe to a topic
- `poll!`: Read messages ready for consumption


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
