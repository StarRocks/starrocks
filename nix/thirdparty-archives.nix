{ pkgs, lib }:

let
  archives = {
    "abseil-cpp-20220623.0.tar.gz" = {
      url = "https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.0.tar.gz";
      md5 = "955b6faedf32ec2ce1b7725561d15618";
      sha256 = "00nnmg2mazx8i1brxzvbmfq5j7738md884373nx8jq0096di4222";
    };
    "arrow-apache-arrow-19.0.1.tar.gz" = {
      url = "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-19.0.1.tar.gz";
      md5 = "8c5091da0f8fb41a47d7f4dad7b712df";
      sha256 = "0snqca1wlkxc22n8k2gs21g6py8r575hwwgqns3cqhc8jl28b2ac";
    };
    "avro-release-1.12.0.tar.gz" = {
      url = "https://github.com/apache/avro/archive/refs/tags/release-1.12.0.tar.gz";
      md5 = "0657ab3ab89d264ccccbca317dbfa54b";
      sha256 = "0mbmg3mcz68x8z47wqn8q8z8id17miyn5gklvfnrbyl15sf2z0si";
    };
    "aws-sdk-cpp-1.11.267.tar.gz" = {
      url = "https://github.com/aws/aws-sdk-cpp/archive/refs/tags/1.11.267.tar.gz";
      md5 = "fdf43e7262f9d08968eb34f9ad18b8e7";
      sha256 = "0pgj5588wyzd8f08ijc5im4asxmzgl7wyfszg0smlk0yjbyb5dw9";
    };
    "azure-storage-files-shares_12.12.0.tar.gz" = {
      url = "https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/azure-storage-files-shares_12.12.0.tar.gz";
      md5 = "cb38786198aa103295d4d670604a9a60";
      sha256 = "03xwi0wpwcg0cjggkzn46i0fy1qnx5gyhbz7y9i16dimqj3zjdyj";
    };
    "benchgen-26.03.11.tar.gz" = {
      url = "https://github.com/StarRocks/benchgen/archive/refs/tags/v26.03.11.tar.gz";
      md5 = "fd97eb82eb4c629d7916b6d012c7e81d";
      sha256 = "0ijzlp185hi43rdqxjcdhvkgvk5vbrn9k17p2ysanhq4pvy0qlzc";
    };
    "bitshuffle-0.5.1.tar.gz" = {
      url = "https://github.com/kiyo-masui/bitshuffle/archive/0.5.1.tar.gz";
      md5 = "b3bf6a9838927f7eb62214981c138e2f";
      sha256 = "0w2qiymnvkm91lhkhdxhinnhbxfwz7a2f63vbi0m2kn2sjjslc96";
    };
    "BLAKE3-1.8.5.tar.gz" = {
      url = "https://github.com/BLAKE3-team/BLAKE3/archive/refs/tags/1.8.5.tar.gz";
      md5 = "3731247eb9086571ba7128a794c1d2d3";
      sha256 = "1saga405wbgjik49ycsfiap36b2c7yn42vf6x9dmi872hq9dh2r2";
    };
    "boost_1_80_0.tar.gz" = {
      url = "https://archives.boost.io/release/1.80.0/source/boost_1_80_0.tar.gz";
      md5 = "077f074743ea7b0cb49c6ed43953ae95";
      sha256 = "0iqqbv20rqka9d9m3wkc51jzxvqq42nakpn3y5bmh7yxigwkc8ab";
    };
    "boost_1_86_0.tar.gz" = {
      url = "https://archives.boost.io/release/1.86.0/source/boost_1_86_0.tar.gz";
      md5 = "ac857d73bb754b718a039830b07b9624";
      sha256 = "07x03iclnnqflhnr3cbjwq72ly5mppl1wb5cmc5wvw9yzi7yfx95";
    };
    "breakpad-2024.02.16.tar.gz" = {
      url = "https://github.com/google/breakpad/archive/refs/tags/v2024.02.16.tar.gz";
      md5 = "ae8c55b23c157771922b5ddca3803055";
      sha256 = "0a3vahwmr2210vvyxjwrjzjm5m58mxb68xyi6b9z28hq4gchr55i";
    };
    "brotli-1.0.9.tar.gz" = {
      url = "https://github.com/google/brotli/archive/v1.0.9.tar.gz";
      md5 = "c2274f0c7af8470ad514637c35bcee7d";
      sha256 = "0ipyyv6sjsns62czjpq9774kiy2l6cmg96jjh78ndfh50hfxis7r";
    };
    "brpc-1.9.0.tar.gz" = {
      url = "https://github.com/apache/brpc/archive/refs/tags/1.9.0.tar.gz";
      md5 = "a2b626d96a5b017f2a6701ffa594530c";
      sha256 = "0x7azbn9064xbjzb67a2zyvhf078z5lny49ld0ly2wv746h6v1c5";
    };
    "bzip2-1.0.8.tar.gz" = {
      url = "https://fossies.org/linux/misc/bzip2-1.0.8.tar.gz";
      md5 = "67e051268d0c475ea773822f7500d0e5";
      sha256 = "0s92986cv0p692icqlw1j42y9nld8zd83qwhzbqd61p1dqbh6nmb";
    };
    "cares-1_19_1.tar.gz" = {
      url = "https://github.com/c-ares/c-ares/archive/tags/cares-1_19_1.tar.gz";
      md5 = "ae2177836c9dbbacb8f303d167fe700f";
      sha256 = "1pz4v3dkbgsaw0frw1blkvmwsfhblg2j5l0khw7yayf2wxwj4hla";
    };
    "cctz-2.3.tar.gz" = {
      url = "https://github.com/google/cctz/archive/v2.3.tar.gz";
      md5 = "209348e50b24dbbdec6d961059c2fc92";
      sha256 = "1nrjix26whh3ichgdxc0d3bdbmz312ra74rv3hkjmq1k9q6v45c6";
    };
    "CRoaring-4.2.1.tar.gz" = {
      url = "https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.2.1.tar.gz";
      md5 = "00667266a60709978368cf867fb3a3aa";
      sha256 = "0k57qjcia19di0z1ll7rw9dwcn5l5qq3gqx902y0vjdqks77451m";
    };
    "curl-8.16.0.tar.gz" = {
      url = "https://curl.se/download/curl-8.16.0.tar.gz";
      md5 = "3db9de72cc8f04166fa02d3173ac78bb";
      sha256 = "0w5rhly6phhlmm9kiv23hifizg2bz2n01yswzjjabv1rdr3j07m2";
    };
    "curl-8.4.0.tar.gz" = {
      url = "https://curl.se/download/curl-8.4.0.tar.gz";
      md5 = "533e8a3b1228d5945a6a512537bea4c7";
      sha256 = "09z4m7qkkvzf6yhc0bgvpc8h49gsl5snmw60x22z4gq4kj042vl1";
    };
    "cyrus-sasl-2.1.28.tar.gz" = {
      url = "https://github.com/cyrusimap/cyrus-sasl/archive/refs/tags/cyrus-sasl-2.1.28.tar.gz";
      md5 = "7dcf3919b3085a1d09576438171bda91";
      sha256 = "0hf39snnh2fq0dwjilz0rm4jaw3sjfjzdd48ahx1ikmr60x96f1y";
    };
    "datasketches-cpp-4.0.0.tar.gz" = {
      url = "https://github.com/apache/datasketches-cpp/archive/refs/tags/4.0.0.tar.gz";
      md5 = "724cd1df9735de2b8939d298f0d95ea2";
      sha256 = "04ng0l997j8vxvsp60haf6nvhx4201qsrkhzm3y35qr57k3f4mh4";
    };
    "fast-float-3.5.1.tar.gz" = {
      url = "https://github.com/fastfloat/fast_float/archive/refs/tags/v3.5.1.tar.gz";
      md5 = "adb3789b99f47e0cd971b4d90727d4d0";
      sha256 = "1nad5fihmv4g2i7cih7nwkv9rba9gw81lill7k8gglnccsfbyn45";
    };
    "FlameGraph-20251015.tar.gz" = {
      url = "https://github.com/murphyatwork/FlameGraph/archive/refs/tags/20251015.tar.gz";
      md5 = "bddefda5f1271a3dd5324b02ad61d4a5";
      sha256 = "19c123dfiv3lfsm0yqdj0imz0lpgvgbjvsp6fq945zd6dq1h07gx";
    };
    "flatbuffers-v1.10.0.tar.gz" = {
      url = "https://github.com/google/flatbuffers/archive/v1.10.0.tar.gz";
      md5 = "f7d19a3f021d93422b0bc287d7148cd2";
      sha256 = "0z4swldxs0s31hnkqdhsbfmc8vx3p7zsvmqaw4l31r2iikdy651p";
    };
    "fmt-10.2.1.zip" = {
      url = "https://github.com/fmtlib/fmt/releases/download/10.2.1/fmt-10.2.1.zip";
      md5 = "04e266ad52659480d593486a17eed804";
      sha256 = "1j8nln7rql2nxkhdlgpmx1c1dp6dyxnar1n5r7sjg0rws6i5289i";
    };
    "gflags-2.2.2.tar.gz" = {
      url = "https://github.com/gflags/gflags/archive/v2.2.2.tar.gz";
      md5 = "1a865b93bacfa963201af3f75b7bd64c";
      sha256 = "1ksdqrk2jjcrqkgxkp6jj8vf8k5i794x5p1b6lxm2rvkrwajzbrl";
    };
    "glog-0.7.1.tar.gz" = {
      url = "https://github.com/google/glog/archive/v0.7.1.tar.gz";
      md5 = "128e2995cc33d794ff24f785a3060346";
      sha256 = "021vs7cbz22nkzshc0324cjb28v6y68y8hcsa4pn3rxphxzair00";
    };
    "google_benchmark-1.9.5.tar.gz" = {
      url = "https://github.com/google/benchmark/archive/refs/tags/v1.9.5.tar.gz";
      md5 = "12c6c0c228fc07106c62634222bd2541";
      sha256 = "0h53x9vkjizqd7lyr121g4hr1xj1dfrghlgrps4a5i5sh8f38ccn";
    };
    "googletest-release-1.10.0.tar.gz" = {
      url = "https://github.com/google/googletest/archive/release-1.10.0.tar.gz";
      md5 = "ecd1fa65e7de707cd5c00bdac56022cd";
      sha256 = "1jvdf56gcvhyi9fj70gcigxva2sad6dfmnj3grxfql8mk9x1bjcx";
    };
    "gperftools-2.7.tar.gz" = {
      url = "https://github.com/gperftools/gperftools/archive/gperftools-2.7.tar.gz";
      md5 = "797e7b7f6663288e2b90ab664861c61a";
      sha256 = "12nkycimixj5p398isicma8znhv295sngjdmgp451m8m8dab921s";
    };
    "grpc-1.43.0.tar.gz" = {
      url = "https://github.com/grpc/grpc/archive/refs/tags/v1.43.0.tar.gz";
      md5 = "92559743e7b5d3f67486c4c0de2f5cbe";
      sha256 = "1p6xbaq3y3z28zq3l50q3bjjp0f74my93h1fm6plvslwd4624iwn";
    };
    "hadoop-3.4.3-src.tar.gz" = {
      url = "https://archive.apache.org/dist/hadoop/common/hadoop-3.4.3/hadoop-3.4.3-src.tar.gz";
      md5 = "c5ac53ca70cc667189ec824c6048914a";
      sha256 = "1d38yknmvvym19s8hia14pkrccqr7lq5iclm9j9hqvpyx0h0ww0l";
    };
    "hyperscan-5.3.0.aarch64.tar.gz" = {
      url = "https://github.com/kunpengcompute/hyperscan/archive/refs/tags/v5.3.0.aarch64.tar.gz";
      md5 = "ef337257bde6583242a739fab6fb161f";
      sha256 = "1cawqr9yx7b02kk8ihvf41ps7wv4qzi7gi4jw53x13rnnfyva170";
    };
    "hyperscan-5.4.0.tar.gz" = {
      url = "https://github.com/intel/hyperscan/archive/v5.4.0.tar.gz";
      md5 = "65e08385038c24470a248f6ff2fa379b";
      sha256 = "0vqhz8zj8ra2ray1dg6vnmipgyi7s4250bl5c8891qs7mwwvl6p5";
    };
    "icu4c-76_1-src.zip" = {
      url = "https://github.com/unicode-org/icu/releases/download/release-76-1/icu4c-76_1-src.zip";
      md5 = "f5f5c827d94af8445766c7023aca7f6b";
      sha256 = "0gzv2yc2vdc2qwyfzdcz2ja7sjlm42i20kypgfhcb8nxhlhr988l";
    };
    "jansson-2.14.tar.gz" = {
      url = "https://github.com/akheron/jansson/releases/download/v2.14/jansson-2.14.tar.gz";
      md5 = "6cbfc54c2ab3b4d7284e188e185c2b0b";
      sha256 = "0acr9f6m6jdbhlqbq5hx321d1j234lpznv13crmxgy0wwh8d162p";
    };
    "jemalloc-5.3.0.tar.bz2" = {
      url = "https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2";
      md5 = "09a8328574dab22a7df848eae6dbbf53";
      sha256 = "1apyxjd1ixy4g8xkr61p0ny8jiz8vyv1j0k4nxqkxpqrf4g2vf1d";
    };
    "krb5-1.19.4.tar.gz" = {
      url = "https://kerberos.org/dist/krb5/1.19/krb5-1.19.4.tar.gz";
      md5 = "ef76083e58f8c49066180642d7c2814a";
      sha256 = "1sslkcbzg4dc98957lhzchwkfnyd2shpkrip75ms5q2db8f9ixa1";
    };
    "leveldb-1.20.tar.gz" = {
      url = "https://github.com/google/leveldb/archive/v1.20.tar.gz";
      md5 = "298b5bddf12c675d6345784261302252";
      sha256 = "0r36bcrj6b2afsp4aw1gjai3jbs1c7734pxpc1jz7hh9nasyiazm";
    };
    "libdeflate-1.18.zip" = {
      url = "https://github.com/ebiggers/libdeflate/archive/refs/tags/v1.18.zip";
      md5 = "1ec42dfe7d777929ade295281560d750";
      sha256 = "0y1vcvv5s3iwip1xskhggxsgyc6ivb2ajdddahzfif5gh4c5ijqr";
    };
    "libdivide-v5.2.0.tar.gz" = {
      url = "https://github.com/ridiculousfish/libdivide/archive/refs/tags/v5.2.0.tar.gz";
      md5 = "4ba77777192c295d6de2b86d88f3239a";
      sha256 = "0j9zf35rid8blfqp1irzqkmn8x22wzhy1hgjglxq5nnv9h693bkk";
    };
    "libevent-24236aed01798303745470e6c498bf606e88724a.zip" = {
      url = "https://github.com/libevent/libevent/archive/24236ae.zip";
      md5 = "c6c4e7614f03754b8c67a17f68177649";
      sha256 = "061j0hmk9sf3klx5i7xi1zkjwvckbizx2xnr0if68h50dwy2nqvc";
    };
    "libfiu-1.1.tar.gz" = {
      url = "https://blitiri.com.ar/p/libfiu/files/1.1/libfiu-1.1.tar.gz";
      md5 = "51092dcb7801efb511b7b962388d9ff4";
      sha256 = "10sxmlhlb3f4smvwaaqvxsxwafi1gb58c5k60mw4cbx9zm4jglsp";
    };
    "librdkafka-2.11.0.tar.gz" = {
      url = "https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.11.0.tar.gz";
      md5 = "bc611d0340e269abaa8886d42ff9c558";
      sha256 = "1jlmjc5lbrr5dxf82rzjza7wiq6llq6p13xws7gd96n0qwyq4ajr";
    };
    "libserdes-7.3.1.tar.gz" = {
      url = "https://github.com/confluentinc/libserdes/archive/refs/tags/v7.3.1.tar.gz";
      md5 = "61012487a8845f37540710ac4ac2f7ab";
      sha256 = "1s0j3v3jdqh8zrinp5dm0zvlra9vjr0q81ibbb3gwsqdvqa1jcz6";
    };
    "libxml2-2.11.7.tar.gz" = {
      url = "https://github.com/GNOME/libxml2/archive/refs/tags/v2.11.7.tar.gz";
      md5 = "929dade129bbe7072e65c3121fbf12c2";
      sha256 = "1rdcn44gmp6zb3b70r5w8dzg1wk0yh9lmqmx0wn6r4bwj2g2bdig";
    };
    "llvm-project-18.1.8.src.tar.xz" = {
      url = "https://github.com/llvm/llvm-project/releases/download/llvmorg-18.1.8/llvm-project-18.1.8.src.tar.xz";
      md5 = "81cd0be5ae6f1ad8961746116d426a96";
      sha256 = "0aigvd3i4qbcxcj3k0yjqh7pxn0jjadsacymr2byxkijdmx5an0b";
    };
    "lz4-1.10.0.tar.gz" = {
      url = "https://github.com/lz4/lz4/archive/v1.10.0.tar.gz";
      md5 = "dead9f5f1966d9ae56e1e32761e4e675";
      sha256 = "12zlqlp7j3fri1bvwfpz7637cvf6iv7mq18j54imxcs48y814xak";
    };
    "lzo-2.10.tar.gz" = {
      url = "http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz";
      md5 = "39d3f3f9c55c87b1e5d6888e1420f4b5";
      sha256 = "0wm04519pd3g8hqpjqhfr72q8qmbiwqaxcs3cndny9h86aa95y60";
    };
    "mariadb-connector-c-3.1.14.tar.gz" = {
      url = "https://github.com/mariadb-corporation/mariadb-connector-c/archive/refs/tags/v3.1.14.tar.gz";
      md5 = "86c4052adeb8447900bf33b4e2ddd1f9";
      sha256 = "13ia4a1zh9r22pnwsdi55wax9kj9y8b9b02lis0rfw269j1g0596";
    };
    "openssl-OpenSSL_1_1_1m.tar.gz" = {
      url = "https://github.com/openssl/openssl/archive/OpenSSL_1_1_1m.tar.gz";
      md5 = "710c2368d28f1a25ab92e25b5b9b11ec";
      sha256 = "0q5kvar91asbv7k203sx3rafqirf4rhqih3anz829a7hgjnj9bin";
    };
    "opentelemetry-cpp-v1.2.0.tar.gz" = {
      url = "https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.2.0.tar.gz";
      md5 = "c084abc742c6b3cd4c9c3684e559d4e1";
      sha256 = "1hlplg4fzq96y725qvh8r7n1v9vd25g0xqq6bcdbhi7swkwj0r3s";
    };
    "poco-1.12.5-release.tar.gz" = {
      url = "https://github.com/pocoproject/poco/archive/refs/tags/poco-1.12.5-release.tar.gz";
      md5 = "282e54a68911f516b15d07136c78592b";
      sha256 = "19hprnr6c87490m8531vl9ygnhzbkw7whz1yy1lk09njzjq8xccj";
    };
    "protobuf-3.14.0.tar.gz" = {
      url = "https://github.com/google/protobuf/archive/v3.14.0.tar.gz";
      md5 = "0c9d2a96f3656ba7ef3b23b533fb6170";
      sha256 = "04v1q7g6kx9nwm1fs8dix27iszh3ycnsidf8wry00mnns02zdxfh";
    };
    "pulsar-client-3.3.0.tar.gz" = {
      url = "https://github.com/apache/pulsar-client-cpp/archive/refs/tags/v3.3.0.tar.gz";
      md5 = "348b7e5ec39e50547668520d13a417a1";
      sha256 = "1hwv0s3v6hmf6c4rikpaci1fgvyj01vbqz47dx84ac54pcyp55xy";
    };
    "ragel-6.10.tar.gz" = {
      url = "https://www.colm.net/files/ragel/ragel-6.10.tar.gz";
      md5 = "748cae8b50cffe9efcaa5acebc6abf0d";
      sha256 = "0gvcsl62gh6sg73nwaxav4a5ja23zcnyxncdcdnqa2yjcpdnw5az";
    };
    "rapidjson-1.1.0.tar.gz" = {
      url = "https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz";
      md5 = "badd12c511e081fec6c89c43a7027bce";
      sha256 = "13nrpvw8f1wx0ga7svbzld7pgrv8l172nangpipnj7jaf0lysz5z";
    };
    "re2-2022-12-01.tar.gz" = {
      url = "https://github.com/google/re2/archive/refs/tags/2022-12-01.tar.gz";
      md5 = "f25d7b06a3e7747ecbb2f12d48be61cd";
      sha256 = "01jmmkxsbrnsncm0biaj2dhvs8nlbi037pfx8qmxnml1csv6anv6";
    };
    "rocksdb-6.22.1.zip" = {
      url = "https://github.com/facebook/rocksdb/archive/refs/tags/v6.22.1.zip";
      md5 = "02727e52cdb94fa6a9dbbd68d157e619";
      sha256 = "1dd4mbx05cymqa6vm66ihk4wzk55fm3bh7x0b9c2hdfcqw8q04mw";
    };
    "ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip" = {
      url = "https://github.com/ulfjack/ryu/archive/aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip";
      md5 = "cb82b6da904d919470fe3f5a01ca30ff";
      sha256 = "0zj9sbvcnqd3fls14r4mwd41fqf39xpzxigwzn0zrvxdwznjahad";
    };
    "s2geometry-0.9.0.tar.gz" = {
      url = "https://github.com/google/s2geometry/archive/v0.9.0.tar.gz";
      md5 = "293552c7646193b8b4a01556808fe155";
      sha256 = "0qlhv3qkgh63yls1f6qylnsg69lfavm61ymz564rx4k87xjrph2l";
    };
    "simdjson-v3.9.4.tar.gz" = {
      url = "https://github.com/simdjson/simdjson/archive/refs/tags/v3.9.4.tar.gz";
      md5 = "bdc1dfcb2a89dc0c09e8370808a946f5";
      sha256 = "0byazknlr5x941n9ba3j14prf29yk2dw7nwh3a4wbqd11zh3pwcv";
    };
    "simdutf-5.2.8.tar.gz" = {
      url = "https://github.com/simdutf/simdutf/archive/refs/tags/v5.2.8.tar.gz";
      md5 = "731c78ab5a10c6073942dc93d5c4b04c";
      sha256 = "0xwdx99qn4ckv4bd9ib5w3i00l8wiy2d7vrdz2c8avasz2zg21i7";
    };
    "snappy-1.1.8.tar.gz" = {
      url = "https://github.com/google/snappy/archive/1.1.8.tar.gz";
      md5 = "70e48cba7fecf289153d009791c9977f";
      sha256 = "07v5b365vz6bjdlqw4vwcna4yhaf6xzxny31hfq159ijg3q7gdhn";
    };
    "starrocks-clucene-2026.04.09.tar.gz" = {
      url = "https://github.com/StarRocks/clucene/archive/refs/tags/starrocks-2026.04.09.tar.gz";
      md5 = "a06ce32908dad5b2b834b19a8879a2c8";
      sha256 = "1a40dmgchxcbalxr1xvl1ma3k11z34qa934v4f0i0az3kqpcyrmb";
    };
    "streamvbyte-0.5.1.tar.gz" = {
      url = "https://github.com/lemire/streamvbyte/archive/refs/tags/v0.5.1.tar.gz";
      md5 = "251d9200d27dda9120653b4928a23a86";
      sha256 = "0287mwpqc83rgs7rmc3iilsl6bnnldf8791ppgs7j2zrh8sl1wmh";
    };
    "thrift-0.23.0.tar.gz" = {
      url = "https://archive.apache.org/dist/thrift/0.23.0/thrift-0.23.0.tar.gz";
      md5 = "7b62f4258ded41e233a638fe8b9fcf64";
      sha256 = "0vzlx0xv0g8xs6zz4l7zll8664cc40qnj6asdk8i67xfs8rdjn8q";
    };
    "vectorscan-5.4.12.tar.gz" = {
      url = "https://github.com/VectorCamp/vectorscan/archive/refs/tags/vectorscan/5.4.12.tar.gz";
      md5 = "384eab5b23831993df96e5fa55f9951e";
      sha256 = "1vdkr5z53cln2qvxszyrzy0v2imjlqil9b07y5rkj5mc730g7i0s";
    };
    "velocypack-XYZ1.0.tar.gz" = {
      url = "https://github.com/arangodb/velocypack/archive/refs/tags/XYZ1.0.tar.gz";
      md5 = "161cbf4c347f6daadacfb749c31842f8";
      sha256 = "1g943v85ipgyxb62hrn54fsc5aicihrp6rwvj0pb6p19l181swym";
    };
    "xsimd-13.2.0.tar.gz" = {
      url = "https://github.com/xtensor-stack/xsimd/archive/refs/tags/13.2.0.tar.gz";
      md5 = "f451a1c57d2a4fdc0ba663be438dced4";
      sha256 = "0yj4hbdfrhwkdny6fb7yq5jbw6pldp1m671jf3f5l64cahywvn7d";
    };
    "xxHash-0.8.3.tar.gz" = {
      url = "https://github.com/Cyan4973/xxHash/archive/refs/tags/v0.8.3.tar.gz";
      md5 = "599804eb9555e51c05f1b821f9212a07";
      sha256 = "104syb3m147ksf1lad4jqci0gww2xwc7g989v42zsg91x3ghirma";
    };
    "zlib-ng-2.3.3.tar.gz" = {
      url = "https://github.com/zlib-ng/zlib-ng/archive/refs/tags/2.3.3.tar.gz";
      md5 = "72337e6a7d2662af50a4ed0274c61b7e";
      sha256 = "1l87r6h8qccba1yjx6n13q36yh0wrq3rzz9nnraq5ssjr2lmmipr";
    };
    "zstd-1.5.7.tar.gz" = {
      url = "https://github.com/facebook/zstd/archive/v1.5.7.tar.gz";
      md5 = "619a019adbbc4536e7fb93cdbb01af3e";
      sha256 = "1qrf074w0wlykrj3pgfs5qp90s374a05pa0wdvjm82djar2jimrp";
    };
  };

  awsCrtArchives = [
    {
      name = "aws-crt-cpp";
      fileName = "aws-crt-cpp-e4514b7fb8b1fe67429aa7b0e00f628999722174.zip";
      url = "https://codeload.github.com/awslabs/aws-crt-cpp/zip/e4514b7fb8b1fe67429aa7b0e00f628999722174";
      sha256 = "sha256-9MdK0nOOyGkkOQ5XH0W2Fo19p8nbJ1S5aL41yoydCNY=";
    }
    {
      name = "aws-c-auth";
      fileName = "aws-c-auth-6ba7a0f8688c713dfe137716dbd5be324c2315b0.zip";
      url = "https://codeload.github.com/awslabs/aws-c-auth/zip/6ba7a0f8688c713dfe137716dbd5be324c2315b0";
      sha256 = "sha256-2ZbYkDcTbff3C/XZ6O2tvizeXGuMa3GjcQfqsihPSME=";
    }
    {
      name = "aws-c-cal";
      fileName = "aws-c-cal-56f0a79ceb10f2efcf92f525ace717f84d8c8a11.zip";
      url = "https://codeload.github.com/awslabs/aws-c-cal/zip/56f0a79ceb10f2efcf92f525ace717f84d8c8a11";
      sha256 = "sha256-ocVtkdo90qpCYGQJ8l8TQW7dtM9IyMH5yCS9b5KjcIE=";
    }
    {
      name = "aws-c-common";
      fileName = "aws-c-common-8eaa0986ad3cfd46c87432a2e4c8ab81a786085f.zip";
      url = "https://codeload.github.com/awslabs/aws-c-common/zip/8eaa0986ad3cfd46c87432a2e4c8ab81a786085f";
      sha256 = "sha256-UuCdrkLYRWYkCzc7Me1bP7xw3NMp9Nm5frbTbL2Aei4=";
    }
    {
      name = "aws-c-compression";
      fileName = "aws-c-compression-99ec79ee2970f1a045d4ced1501b97ee521f2f85.zip";
      url = "https://codeload.github.com/awslabs/aws-c-compression/zip/99ec79ee2970f1a045d4ced1501b97ee521f2f85";
      sha256 = "sha256-7WoVgE97s+K6EH2Tr+lNy0AT3Lforus6ggd14hGHF4w=";
    }
    {
      name = "aws-c-event-stream";
      fileName = "aws-c-event-stream-63d1e1021b04ce3c3b1fc1895078ac85e0430b24.zip";
      url = "https://codeload.github.com/awslabs/aws-c-event-stream/zip/63d1e1021b04ce3c3b1fc1895078ac85e0430b24";
      sha256 = "sha256-YebplulHKkFVbYogYGpsxx9vh/nVwlHeqPUCpGys8BQ=";
    }
    {
      name = "aws-c-http";
      fileName = "aws-c-http-6a1c157c20640a607102738909e89561a41e91e9.zip";
      url = "https://codeload.github.com/awslabs/aws-c-http/zip/6a1c157c20640a607102738909e89561a41e91e9";
      sha256 = "sha256-qLNpZNjA4c6xbrd6bO2vSCIlnNZvvH1lmWn4MFPTuAA=";
    }
    {
      name = "aws-c-io";
      fileName = "aws-c-io-6225ebb9da28f1023ad5e21694de9d165cd65f3b.zip";
      url = "https://codeload.github.com/awslabs/aws-c-io/zip/6225ebb9da28f1023ad5e21694de9d165cd65f3b";
      sha256 = "sha256-ZnqXN5PwtWvcNCWtvm/BM0QjG7jmBT28ZhGSS8dinAY=";
    }
    {
      name = "aws-c-mqtt";
      fileName = "aws-c-mqtt-17ee24a2177fc64cf9773d430a24e6fa06a89dd0.zip";
      url = "https://codeload.github.com/awslabs/aws-c-mqtt/zip/17ee24a2177fc64cf9773d430a24e6fa06a89dd0";
      sha256 = "sha256-mEebEd+3JeEeFSY/mLU9rfUP6+KRASRne6brERsRnlw=";
    }
    {
      name = "aws-c-s3";
      fileName = "aws-c-s3-1dd55be83b19a55cd9c155e2da977cdc76112a91.zip";
      url = "https://codeload.github.com/awslabs/aws-c-s3/zip/1dd55be83b19a55cd9c155e2da977cdc76112a91";
      sha256 = "sha256-dQ8tQwR2Xn6oj9n+ExbpEdy29yF51kUVdaUcnlFZ+hw=";
    }
    {
      name = "aws-c-sdkutils";
      fileName = "aws-c-sdkutils-fd8c0ba2e233997eaaefe82fb818b8b444b956d3.zip";
      url = "https://codeload.github.com/awslabs/aws-c-sdkutils/zip/fd8c0ba2e233997eaaefe82fb818b8b444b956d3";
      sha256 = "sha256-deMc7gYjITzRCE0oNuyqOhQiF0nmofVWzapRgHKIhHQ=";
    }
    {
      name = "aws-checksums";
      fileName = "aws-checksums-321b805559c8e911be5bddba13fcbd222a3e2d3a.zip";
      url = "https://codeload.github.com/awslabs/aws-checksums/zip/321b805559c8e911be5bddba13fcbd222a3e2d3a";
      sha256 = "sha256-HZAxPZWoMNk9hxE71+yA5Z18FbFW+9K1Q7sv3t7WitE=";
    }
    {
      name = "aws-lc";
      fileName = "aws-lc-dc4e28145ceb6d46b5475e833f2da8def6d583fe.zip";
      url = "https://codeload.github.com/awslabs/aws-lc/zip/dc4e28145ceb6d46b5475e833f2da8def6d583fe";
      sha256 = "sha256-TeecD45bAZv2tZ9qmq0+M34jeU+Jmzu18cntQVVi79A=";
    }
    {
      name = "s2n";
      fileName = "s2n-0998358a6ef7c4f22295deba088796fe354c5f4c.zip";
      url = "https://codeload.github.com/awslabs/s2n/zip/0998358a6ef7c4f22295deba088796fe354c5f4c";
      sha256 = "sha256-4r57nUbltvSxnRgYkGdtnCS7evbQWRdSK0EWea0kAFw=";
    }
  ];

  bySystem = {
    "x86_64-linux" = [
      "starrocks-clucene-2026.04.09.tar.gz"
      "libevent-24236aed01798303745470e6c498bf606e88724a.zip"
      "openssl-OpenSSL_1_1_1m.tar.gz"
      "thrift-0.23.0.tar.gz"
      "protobuf-3.14.0.tar.gz"
      "gflags-2.2.2.tar.gz"
      "glog-0.7.1.tar.gz"
      "googletest-release-1.10.0.tar.gz"
      "rapidjson-1.1.0.tar.gz"
      "simdjson-v3.9.4.tar.gz"
      "snappy-1.1.8.tar.gz"
      "gperftools-2.7.tar.gz"
      "zlib-ng-2.3.3.tar.gz"
      "lz4-1.10.0.tar.gz"
      "bzip2-1.0.8.tar.gz"
      "curl-8.4.0.tar.gz"
      "re2-2022-12-01.tar.gz"
      "boost_1_80_0.tar.gz"
      "leveldb-1.20.tar.gz"
      "brpc-1.9.0.tar.gz"
      "rocksdb-6.22.1.zip"
      "krb5-1.19.4.tar.gz"
      "cyrus-sasl-2.1.28.tar.gz"
      "librdkafka-2.11.0.tar.gz"
      "pulsar-client-3.3.0.tar.gz"
      "flatbuffers-v1.10.0.tar.gz"
      "arrow-apache-arrow-19.0.1.tar.gz"
      "brotli-1.0.9.tar.gz"
      "zstd-1.5.7.tar.gz"
      "s2geometry-0.9.0.tar.gz"
      "bitshuffle-0.5.1.tar.gz"
      "CRoaring-4.2.1.tar.gz"
      "jemalloc-5.3.0.tar.bz2"
      "cctz-2.3.tar.gz"
      "fmt-10.2.1.zip"
      "ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
      "breakpad-2024.02.16.tar.gz"
      "hadoop-3.4.3-src.tar.gz"
      "ragel-6.10.tar.gz"
      "hyperscan-5.4.0.tar.gz"
      "mariadb-connector-c-3.1.14.tar.gz"
      "aws-sdk-cpp-1.11.267.tar.gz"
      "velocypack-XYZ1.0.tar.gz"
      "opentelemetry-cpp-v1.2.0.tar.gz"
      "google_benchmark-1.9.5.tar.gz"
      "fast-float-3.5.1.tar.gz"
      "streamvbyte-0.5.1.tar.gz"
      "jansson-2.14.tar.gz"
      "avro-release-1.12.0.tar.gz"
      "libserdes-7.3.1.tar.gz"
      "lzo-2.10.tar.gz"
      "datasketches-cpp-4.0.0.tar.gz"
      "libfiu-1.1.tar.gz"
      "libdeflate-1.18.zip"
      "llvm-project-18.1.8.src.tar.xz"
      "abseil-cpp-20220623.0.tar.gz"
      "cares-1_19_1.tar.gz"
      "grpc-1.43.0.tar.gz"
      "simdutf-5.2.8.tar.gz"
      "poco-1.12.5-release.tar.gz"
      "icu4c-76_1-src.zip"
      "xsimd-13.2.0.tar.gz"
      "libxml2-2.11.7.tar.gz"
      "azure-storage-files-shares_12.12.0.tar.gz"
      "libdivide-v5.2.0.tar.gz"
      "FlameGraph-20251015.tar.gz"
      "xxHash-0.8.3.tar.gz"
      "BLAKE3-1.8.5.tar.gz"
      "benchgen-26.03.11.tar.gz"
    ];
    "aarch64-linux" = [
      "starrocks-clucene-2026.04.09.tar.gz"
      "libevent-24236aed01798303745470e6c498bf606e88724a.zip"
      "openssl-OpenSSL_1_1_1m.tar.gz"
      "thrift-0.23.0.tar.gz"
      "protobuf-3.14.0.tar.gz"
      "gflags-2.2.2.tar.gz"
      "glog-0.7.1.tar.gz"
      "googletest-release-1.10.0.tar.gz"
      "rapidjson-1.1.0.tar.gz"
      "simdjson-v3.9.4.tar.gz"
      "snappy-1.1.8.tar.gz"
      "gperftools-2.7.tar.gz"
      "zlib-ng-2.3.3.tar.gz"
      "lz4-1.10.0.tar.gz"
      "bzip2-1.0.8.tar.gz"
      "curl-8.4.0.tar.gz"
      "re2-2022-12-01.tar.gz"
      "boost_1_80_0.tar.gz"
      "leveldb-1.20.tar.gz"
      "brpc-1.9.0.tar.gz"
      "rocksdb-6.22.1.zip"
      "krb5-1.19.4.tar.gz"
      "cyrus-sasl-2.1.28.tar.gz"
      "librdkafka-2.11.0.tar.gz"
      "pulsar-client-3.3.0.tar.gz"
      "flatbuffers-v1.10.0.tar.gz"
      "arrow-apache-arrow-19.0.1.tar.gz"
      "brotli-1.0.9.tar.gz"
      "zstd-1.5.7.tar.gz"
      "s2geometry-0.9.0.tar.gz"
      "bitshuffle-0.5.1.tar.gz"
      "CRoaring-4.2.1.tar.gz"
      "jemalloc-5.3.0.tar.bz2"
      "cctz-2.3.tar.gz"
      "fmt-10.2.1.zip"
      "ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
      "hadoop-3.4.3-src.tar.gz"
      "ragel-6.10.tar.gz"
      "hyperscan-5.3.0.aarch64.tar.gz"
      "mariadb-connector-c-3.1.14.tar.gz"
      "aws-sdk-cpp-1.11.267.tar.gz"
      "velocypack-XYZ1.0.tar.gz"
      "opentelemetry-cpp-v1.2.0.tar.gz"
      "google_benchmark-1.9.5.tar.gz"
      "fast-float-3.5.1.tar.gz"
      "streamvbyte-0.5.1.tar.gz"
      "jansson-2.14.tar.gz"
      "avro-release-1.12.0.tar.gz"
      "libserdes-7.3.1.tar.gz"
      "lzo-2.10.tar.gz"
      "datasketches-cpp-4.0.0.tar.gz"
      "libfiu-1.1.tar.gz"
      "llvm-project-18.1.8.src.tar.xz"
      "abseil-cpp-20220623.0.tar.gz"
      "cares-1_19_1.tar.gz"
      "grpc-1.43.0.tar.gz"
      "simdutf-5.2.8.tar.gz"
      "poco-1.12.5-release.tar.gz"
      "icu4c-76_1-src.zip"
      "xsimd-13.2.0.tar.gz"
      "libxml2-2.11.7.tar.gz"
      "azure-storage-files-shares_12.12.0.tar.gz"
      "libdivide-v5.2.0.tar.gz"
      "FlameGraph-20251015.tar.gz"
      "xxHash-0.8.3.tar.gz"
      "BLAKE3-1.8.5.tar.gz"
      "benchgen-26.03.11.tar.gz"
    ];
    "aarch64-darwin" = [
      "starrocks-clucene-2026.04.09.tar.gz"
      "libevent-24236aed01798303745470e6c498bf606e88724a.zip"
      "openssl-OpenSSL_1_1_1m.tar.gz"
      "thrift-0.23.0.tar.gz"
      "protobuf-3.14.0.tar.gz"
      "gflags-2.2.2.tar.gz"
      "glog-0.7.1.tar.gz"
      "googletest-release-1.10.0.tar.gz"
      "rapidjson-1.1.0.tar.gz"
      "simdjson-v3.9.4.tar.gz"
      "snappy-1.1.8.tar.gz"
      "gperftools-2.7.tar.gz"
      "zlib-ng-2.3.3.tar.gz"
      "lz4-1.10.0.tar.gz"
      "bzip2-1.0.8.tar.gz"
      "curl-8.16.0.tar.gz"
      "re2-2022-12-01.tar.gz"
      "boost_1_86_0.tar.gz"
      "leveldb-1.20.tar.gz"
      "brpc-1.9.0.tar.gz"
      "rocksdb-6.22.1.zip"
      "krb5-1.19.4.tar.gz"
      "cyrus-sasl-2.1.28.tar.gz"
      "librdkafka-2.11.0.tar.gz"
      "pulsar-client-3.3.0.tar.gz"
      "flatbuffers-v1.10.0.tar.gz"
      "arrow-apache-arrow-19.0.1.tar.gz"
      "brotli-1.0.9.tar.gz"
      "zstd-1.5.7.tar.gz"
      "s2geometry-0.9.0.tar.gz"
      "bitshuffle-0.5.1.tar.gz"
      "CRoaring-4.2.1.tar.gz"
      "jemalloc-5.3.0.tar.bz2"
      "cctz-2.3.tar.gz"
      "fmt-10.2.1.zip"
      "ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
      "hadoop-3.4.3-src.tar.gz"
      "ragel-6.10.tar.gz"
      "vectorscan-5.4.12.tar.gz"
      "mariadb-connector-c-3.1.14.tar.gz"
      "aws-sdk-cpp-1.11.267.tar.gz"
      "velocypack-XYZ1.0.tar.gz"
      "opentelemetry-cpp-v1.2.0.tar.gz"
      "google_benchmark-1.9.5.tar.gz"
      "fast-float-3.5.1.tar.gz"
      "streamvbyte-0.5.1.tar.gz"
      "jansson-2.14.tar.gz"
      "avro-release-1.12.0.tar.gz"
      "libserdes-7.3.1.tar.gz"
      "lzo-2.10.tar.gz"
      "datasketches-cpp-4.0.0.tar.gz"
      "libfiu-1.1.tar.gz"
      "llvm-project-18.1.8.src.tar.xz"
      "abseil-cpp-20220623.0.tar.gz"
      "cares-1_19_1.tar.gz"
      "grpc-1.43.0.tar.gz"
      "simdutf-5.2.8.tar.gz"
      "poco-1.12.5-release.tar.gz"
      "icu4c-76_1-src.zip"
      "xsimd-13.2.0.tar.gz"
      "libxml2-2.11.7.tar.gz"
      "azure-storage-files-shares_12.12.0.tar.gz"
      "libdivide-v5.2.0.tar.gz"
      "FlameGraph-20251015.tar.gz"
      "xxHash-0.8.3.tar.gz"
      "BLAKE3-1.8.5.tar.gz"
      "benchgen-26.03.11.tar.gz"
    ];
  };

  archiveNamesBySystem = {
    "x86_64-linux" = [
      "CLUCENE"
      "LIBEVENT"
      "OPENSSL"
      "THRIFT"
      "PROTOBUF"
      "GFLAGS"
      "GLOG"
      "GTEST"
      "RAPIDJSON"
      "SIMDJSON"
      "SNAPPY"
      "GPERFTOOLS"
      "ZLIB"
      "LZ4"
      "BZIP"
      "CURL"
      "RE2"
      "BOOST"
      "LEVELDB"
      "BRPC"
      "ROCKSDB"
      "KRB5"
      "SASL"
      "LIBRDKAFKA"
      "PULSAR"
      "FLATBUFFERS"
      "ARROW"
      "BROTLI"
      "ZSTD"
      "S2"
      "BITSHUFFLE"
      "CROARINGBITMAP"
      "JEMALLOC"
      "CCTZ"
      "FMT"
      "RYU"
      "BREAK_PAD"
      "HADOOPSRC"
      "RAGEL"
      "HYPERSCAN"
      "MARIADB"
      "AWS_SDK_CPP"
      "VPACK"
      "OPENTELEMETRY"
      "BENCHMARK"
      "FAST_FLOAT"
      "STREAMVBYTE"
      "JANSSON"
      "AVRO"
      "SERDES"
      "LZO2"
      "DATASKETCHES"
      "FIU"
      "LIBDEFLATE"
      "LLVM"
      "ABSL"
      "CARES"
      "GRPC"
      "SIMDUTF"
      "POCO"
      "ICU"
      "XSIMD"
      "LIBXML2"
      "AZURE"
      "LIBDIVIDE"
      "FLAMEGRAPH"
      "XXHASH"
      "BLAKE3"
      "BENCHGEN"
    ];
    "aarch64-linux" = [
      "CLUCENE"
      "LIBEVENT"
      "OPENSSL"
      "THRIFT"
      "PROTOBUF"
      "GFLAGS"
      "GLOG"
      "GTEST"
      "RAPIDJSON"
      "SIMDJSON"
      "SNAPPY"
      "GPERFTOOLS"
      "ZLIB"
      "LZ4"
      "BZIP"
      "CURL"
      "RE2"
      "BOOST"
      "LEVELDB"
      "BRPC"
      "ROCKSDB"
      "KRB5"
      "SASL"
      "LIBRDKAFKA"
      "PULSAR"
      "FLATBUFFERS"
      "ARROW"
      "BROTLI"
      "ZSTD"
      "S2"
      "BITSHUFFLE"
      "CROARINGBITMAP"
      "JEMALLOC"
      "CCTZ"
      "FMT"
      "RYU"
      "HADOOPSRC"
      "RAGEL"
      "HYPERSCAN"
      "MARIADB"
      "AWS_SDK_CPP"
      "VPACK"
      "OPENTELEMETRY"
      "BENCHMARK"
      "FAST_FLOAT"
      "STREAMVBYTE"
      "JANSSON"
      "AVRO"
      "SERDES"
      "LZO2"
      "DATASKETCHES"
      "FIU"
      "LLVM"
      "ABSL"
      "CARES"
      "GRPC"
      "SIMDUTF"
      "POCO"
      "ICU"
      "XSIMD"
      "LIBXML2"
      "AZURE"
      "LIBDIVIDE"
      "FLAMEGRAPH"
      "XXHASH"
      "BLAKE3"
      "BENCHGEN"
    ];
    "aarch64-darwin" = [
      "CLUCENE"
      "LIBEVENT"
      "OPENSSL"
      "THRIFT"
      "PROTOBUF"
      "GFLAGS"
      "GLOG"
      "GTEST"
      "RAPIDJSON"
      "SIMDJSON"
      "SNAPPY"
      "GPERFTOOLS"
      "ZLIB"
      "LZ4"
      "BZIP"
      "CURL"
      "RE2"
      "BOOST"
      "LEVELDB"
      "BRPC"
      "ROCKSDB"
      "KRB5"
      "SASL"
      "LIBRDKAFKA"
      "PULSAR"
      "FLATBUFFERS"
      "ARROW"
      "BROTLI"
      "ZSTD"
      "S2"
      "BITSHUFFLE"
      "CROARINGBITMAP"
      "JEMALLOC"
      "CCTZ"
      "FMT"
      "RYU"
      "HADOOPSRC"
      "RAGEL"
      "HYPERSCAN"
      "MARIADB"
      "AWS_SDK_CPP"
      "VPACK"
      "OPENTELEMETRY"
      "BENCHMARK"
      "FAST_FLOAT"
      "STREAMVBYTE"
      "JANSSON"
      "AVRO"
      "SERDES"
      "LZO2"
      "DATASKETCHES"
      "FIU"
      "LLVM"
      "ABSL"
      "CARES"
      "GRPC"
      "SIMDUTF"
      "POCO"
      "ICU"
      "XSIMD"
      "LIBXML2"
      "AZURE"
      "LIBDIVIDE"
      "FLAMEGRAPH"
      "XXHASH"
      "BLAKE3"
      "BENCHGEN"
    ];
  };

  packageNamesBySystem = {
    "x86_64-linux" = [
      "libevent"
      "zlib"
      "lz4"
      "lzo2"
      "bzip"
      "openssl"
      "boost"
      "protobuf"
      "gflags"
      "gtest"
      "glog"
      "rapidjson"
      "simdjson"
      "snappy"
      "gperftools"
      "curl"
      "re2"
      "thrift"
      "leveldb"
      "brpc"
      "rocksdb"
      "kerberos"
      "sasl"
      "absl"
      "grpc"
      "flatbuffers"
      "jemalloc"
      "brotli"
      "arrow"
      "librdkafka"
      "pulsar"
      "s2"
      "bitshuffle"
      "croaringbitmap"
      "cctz"
      "fmt"
      "fmt_shared"
      "ryu"
      "hadoop_src"
      "ragel"
      "hyperscan"
      "mariadb"
      "aws_cpp_sdk"
      "vpack"
      "opentelemetry"
      "benchmark"
      "fast_float"
      "streamvbyte"
      "jansson"
      "avro_c"
      "avro_cpp"
      "serdes"
      "datasketches"
      "fiu"
      "llvm"
      "clucene"
      "simdutf"
      "poco"
      "icu"
      "xsimd"
      "libxml2"
      "azure"
      "libdivide"
      "flamegraph"
      "xxhash"
      "blake3"
      "benchgen"
      "breakpad"
      "libdeflate"
    ];
    "aarch64-linux" = [
      "libevent"
      "zlib"
      "lz4"
      "lzo2"
      "bzip"
      "openssl"
      "boost"
      "protobuf"
      "gflags"
      "gtest"
      "glog"
      "rapidjson"
      "simdjson"
      "snappy"
      "gperftools"
      "curl"
      "re2"
      "thrift"
      "leveldb"
      "brpc"
      "rocksdb"
      "kerberos"
      "sasl"
      "absl"
      "grpc"
      "flatbuffers"
      "jemalloc"
      "brotli"
      "arrow"
      "librdkafka"
      "pulsar"
      "s2"
      "bitshuffle"
      "croaringbitmap"
      "cctz"
      "fmt"
      "fmt_shared"
      "ryu"
      "hadoop_src"
      "ragel"
      "hyperscan"
      "mariadb"
      "aws_cpp_sdk"
      "vpack"
      "opentelemetry"
      "benchmark"
      "fast_float"
      "streamvbyte"
      "jansson"
      "avro_c"
      "avro_cpp"
      "serdes"
      "datasketches"
      "fiu"
      "llvm"
      "clucene"
      "simdutf"
      "poco"
      "icu"
      "xsimd"
      "libxml2"
      "azure"
      "libdivide"
      "flamegraph"
      "xxhash"
      "blake3"
      "benchgen"
    ];
    "aarch64-darwin" = [
      "libevent"
      "zlib"
      "lz4"
      "lzo2"
      "bzip"
      "openssl"
      "boost"
      "protobuf"
      "gflags"
      "gtest"
      "glog"
      "rapidjson"
      "simdjson"
      "snappy"
      "gperftools"
      "curl"
      "re2"
      "thrift"
      "leveldb"
      "brpc"
      "rocksdb"
      "kerberos"
      "sasl"
      "absl"
      "grpc"
      "flatbuffers"
      "jemalloc"
      "brotli"
      "arrow"
      "librdkafka"
      "pulsar"
      "s2"
      "bitshuffle"
      "croaringbitmap"
      "cctz"
      "fmt"
      "fmt_shared"
      "ryu"
      "hadoop_src"
      "ragel"
      "hyperscan"
      "mariadb"
      "aws_cpp_sdk"
      "vpack"
      "opentelemetry"
      "benchmark"
      "fast_float"
      "streamvbyte"
      "jansson"
      "avro_c"
      "avro_cpp"
      "serdes"
      "datasketches"
      "fiu"
      "llvm"
      "clucene"
      "simdutf"
      "poco"
      "icu"
      "xsimd"
      "libxml2"
      "azure"
      "libdivide"
      "flamegraph"
      "xxhash"
      "blake3"
      "benchgen"
    ];
  };

in
{
  archiveNamesFor = system: archiveNamesBySystem.${system} or [ ];

  packageNamesFor = system: packageNamesBySystem.${system} or [ ];

  fetchedFor =
    system:
    map (
      name:
      let
        archive = archives.${name};
      in
      {
        inherit name;
        path = pkgs.fetchurl {
          inherit name;
          inherit (archive) url sha256;
        };
      }
    ) (bySystem.${system} or [ ]);

  fetchedAwsCrt = map (archive: {
    inherit (archive) name;
    path = pkgs.fetchurl {
      name = archive.fileName;
      inherit (archive) url sha256;
    };
  }) awsCrtArchives;
}
