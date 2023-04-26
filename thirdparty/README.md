Enable frame pointer
===================

When introducing thirdparty, make sure compiler option `-fno-omit-frame-pointer` is enabled. By default it's disabled, which makes profiling hard.  https://gcc.gnu.org/onlinedocs/gcc-10.3.0/gcc/Optimize-Options.html. The ovehead of it can be offset by observablity.


BPF Performance Tools.pdf Chapter2

> On x86_64 today, most software is compiled with gcc’s defaults, breaking frame pointer stack traces. Last time I studied the performance gain from frame pointer omission in our production environment, it was usually less than one percent, and it was often so close to zero that it was difficult to measure. Many microservices at Netflix are running with the frame pointer reenabled, as the performance wins found by CPU profiling outweigh the tiny loss of performance. 
