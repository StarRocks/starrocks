Enable frame pointer
===================

When introducing thirdparty, make sure compiler option `-fno-omit-frame-pointer` is enabled. By default it's disabled, which makes profiling hard.  https://gcc.gnu.org/onlinedocs/gcc-10.3.0/gcc/Optimize-Options.html. The ovehead of it can be offset by observablity.


BPF Performance Tools.pdf Chapter2

> On x86_64 today, most software is compiled with gcc’s defaults, breaking frame pointer stack traces. Last time I studied the performance gain from frame pointer omission in our production environment, it was usually less than one percent, and it was often so close to zero that it was difficult to measure. Many microservices at Netflix are running with the frame pointer reenabled, as the performance wins found by CPU profiling outweigh the tiny loss of performance. 

PROJ for native GEOMETRY CRS conversion
======================================

Contract 5.5 pins PROJ 9.9.0 and its required SQLite 3.53.4 dependency.
The source URLs and MD5 checksums are in `vars.sh`; SQLite's official
SHA3-256 is `454e45f61c6bd75b7420e7190732dea03ce6639c63ada47bbc592f67fc340338`.
The PROJ archive MD5 matches the OSGeo release checksum.

Build with `./thirdparty/build-thirdparty.sh sqlite3 proj`. The build runs
`test-proj.sh` after installation. Both libraries are static. PROJ embeds the
`proj.db` generated from its pinned source, so BE and CN do not need an
external database file. No grid package is bundled; TIFF, CURL, projsync, and
PROJ network access are disabled. A transformation needing unavailable grid
data must fail rather than download data or use an implicit fallback.

The build functions cover Linux x86_64 and aarch64 and macOS aarch64.
The source and embedded database are X/MIT licensed; the license is also
copied to `installed/share/licenses/proj/COPYING`. SQLite's library and shell
code are public domain. See `LICENSE.txt` and the root `NOTICE.txt`.
