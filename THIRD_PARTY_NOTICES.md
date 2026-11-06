# Third-party notices

Il server di MQTT Event Viewer è distribuito con licenza GNU General Public License v2.0 (vedi [`LICENSE`](LICENSE)).
L'interfaccia web d'esempio in `web/` è distribuita con licenza MIT (vedi [`web/LICENSE`](web/LICENSE)).
Il progetto include o usa i seguenti componenti di terze parti, ciascuno con la propria licenza.

| Componente | Versione | File | Licenza |
| --- | --- | --- | --- |
| Mongoose | 7.14 | `mongoose.c`, `mongoose.h` | GPL-2.0-only (o commerciale Cesanta) |
| cJSON | — | `cJSON.c`, `cJSON.h` | MIT |
| SQLite | di sistema (link) | — | Public domain |

---

## Mongoose

Copyright (c) 2004-2013 Sergey Lyubka
Copyright (c) 2013-2024 Cesanta Software Limited

Mongoose è dual-licensed: GNU General Public License version 2 oppure licenza commerciale
(https://www.mongoose.ws/licensing/). Questo progetto lo usa nei termini della GPL-2.0,
il cui testo completo è nel file [`LICENSE`](LICENSE).

## cJSON

```
Copyright (c) 2009-2017 Dave Gamble and cJSON contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

## SQLite

SQLite è di pubblico dominio (https://sqlite.org/copyright.html). Non è incluso nel
repository: viene linkato dalla libreria di sistema o da quella della toolchain.

## OpenSSL

Il viewer non usa TLS e per default non linka OpenSSL. L'opzione `./configure --with-ssl`
esiste ancora, ma OpenSSL 3.x (Apache-2.0) non è compatibile con la GPL-2.0: un binario
linkato con OpenSSL 3 non deve essere distribuito.
