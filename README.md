# firestream

Kafkaesque streams built on firebase

## Installation

### Connecting to firebase

1. Get the `json` file containing your creditials by following the instruction here [https://firebase.google.com/docs/admin/setup](https://firebase.google.com/docs/admin/setup)  

2. Set the GOOGLE_CLOUD_PROJECT environment to the id of your project

3. Set the FIREBASE_CONFIG environment variable to the contents of your `json` key file. (Sometimes it may be necessary to wrap the key contents in single quotes to escape all the special characters within it. e.g. FIREBASE_CONFIG='`contents-of-json`')

4. You're now good to go.

## Usage

Chill, it's still early days. 

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
