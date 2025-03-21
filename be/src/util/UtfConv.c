/*******************************************************************************************

Function set for UTF StrLwr, StrUpr and case insensitive StrCmp and  StrStr

by Jan Bergström, Alphabet AB, 2021 Open source code

2021-10-21

*******************************************************************************************/
//#define UTF32_TEST 1
#include <stdlib.h>
#include <string.h>

#include <stdint.h>
#define Utf32Char uint32_t		// min 4 bytes (int32_t), uint32_t suggested
#define Utf16Char wchar_t		// min 2 bytes (int16_t), wchar_t suggested
#define Utf8Char unsigned char	// must be 1 byte, 8 bits, can be char, the UTF consortium specify unsigned

// Utf 32
size_t StrLenUtf32(const Utf32Char* str);
Utf32Char* StrCpyUtf32(Utf32Char* dest, const Utf32Char* src);
Utf32Char* StrCatUtf32(Utf32Char* dest, const Utf32Char* src);
int StrnCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2, size_t ztCount);
int StrCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2);
Utf32Char* StrToUprUtf32(Utf32Char* pUtf32);
Utf32Char* StrToLwrUtf32(Utf32Char* pUtf32);
int StrnCiCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2, size_t ztCount);
int StrCiCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2);
Utf32Char* StrCiStrUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2);

// Utf 16
size_t StrLenUtf16(const Utf16Char* str);
Utf16Char* StrCpyUtf16(Utf16Char* dest, const Utf16Char* src);
Utf16Char* StrCatUtf16(Utf16Char* dest, const Utf16Char* src);
int StrnCmpUtf16(const Utf16Char* Utf16s1, const Utf16Char* Utf16s2, size_t ztCount);
int StrCmpUtf16(const Utf16Char* Utf16s1, const Utf16Char* Utf16s2);
size_t CharLenUtf16(const Utf16Char* pUtf16);
Utf16Char* ForwardUtf16Chars(const Utf16Char* pUtf16, size_t ztForwardUtf16Chars);
size_t StrLenUtf32AsUtf16(const Utf32Char* pUtf32);
Utf16Char* Utf32ToUtf16(const Utf32Char* pUtf32);
Utf32Char* Utf16ToUtf32(const Utf16Char* pUtf16);
Utf8Char* Utf16ToUtf8(const Utf16Char* pUtf16);
Utf16Char* Utf16StrMakeUprUtf16Str(const Utf16Char* pUtf16);
Utf16Char* Utf16StrMakeLwrUtf16Str(const Utf16Char* pUtf16);
int StrnCiCmpUtf16(const Utf16Char* pUtf16s1, const Utf16Char* pUtf16s2, size_t ztCount);
int StrCiCmpUtf16(const Utf16Char* pUtf16s1, const Utf16Char* pUtf16s2);
Utf16Char* StrCiStrUtf16(const Utf16Char* pUtf16s1, const Utf16Char* pUtf16s2);

// Utf 8
size_t StrLenUtf8(const Utf8Char* str);
int StrnCmpUtf8(const Utf8Char* Utf8s1, const Utf8Char* Utf8s2, size_t ztCount);
int StrCmpUtf8(const Utf8Char* Utf8s1, const Utf8Char* Utf8s2);
size_t CharLenUtf8(const Utf8Char* pUtf8);
Utf8Char* ForwardUtf8Chars(const Utf8Char* pUtf8, size_t ztForwardUtf8Chars);
size_t StrLenUtf32AsUtf8(const Utf32Char* pUtf32);
Utf8Char* Utf32ToUtf8(const Utf32Char* pUtf32);
Utf32Char* Utf8ToUtf32(const Utf8Char* pUtf8);
Utf16Char* Utf8ToUtf16(const Utf8Char* pUtf8);
Utf8Char* Utf8StrMakeUprUtf8Str(const Utf8Char* pUtf8);
Utf8Char* Utf8StrMakeLwrUtf8Str(const Utf8Char* pUtf8);
int StrnCiCmpUtf8(const Utf8Char* pUtf8s1, const Utf8Char* pUtf8s2, size_t ztCount);
int StrCiCmpUtf8(const Utf8Char* pUtf8s1, const Utf8Char* pUtf8s2);
Utf8Char* StrCiStrUtf8(const Utf8Char* pUtf8s1, const Utf8Char* pUtf8s2);

/*******************************************************************************************

UTF32 Upr/Lwr conversions

*******************************************************************************************/
Utf32Char* StrToUprUtf32(Utf32Char* pUtf32)
{
	Utf32Char* p = pUtf32;
	if (pUtf32 && *pUtf32) {
		while(*p) {
			if ((*p >= 0x61) && (*p <= 0x7a)) /* US ASCII */
				(*p) -= 0x20;
			else {
				switch(*p) {
				case 0x00DF:		//	ß	0xc3 0x9f	LATIN SMALL LETTER SHARP S
					*p = 0x1E9E;	//	ẞ	0xe1 0xba 0x9e	LATIN CAPITAL LETTER SHARP S
					break;
				case 0x00E0:		//	à	0xc3 0xa0	LATIN SMALL LETTER A WITH GRAVE
					*p = 0x00C0;	//	À	0xc3 0x80	LATIN CAPITAL LETTER A WITH GRAVE
					break;
				case 0x00E1:		//	á	0xc3 0xa1	LATIN SMALL LETTER A WITH ACUTE
					*p = 0x00C1;	//	Á	0xc3 0x81	LATIN CAPITAL LETTER A WITH ACUTE
					break;
				case 0x00E2:		//	â	0xc3 0xa2	LATIN SMALL LETTER A WITH CIRCUMFLEX
					*p = 0x00C2;	//	Â	0xc3 0x82	LATIN CAPITAL LETTER A WITH CIRCUMFLEX
					break;
				case 0x00E3:		//	ã	0xc3 0xa3	LATIN SMALL LETTER A WITH TILDE
					*p = 0x00C3;	//	Ã	0xc3 0x83	LATIN CAPITAL LETTER A WITH TILDE
					break;
				case 0x00E4:		//	ä	0xc3 0xa4	LATIN SMALL LETTER A WITH DIAERESIS
					*p = 0x00C4;	//	Ä	0xc3 0x84	LATIN CAPITAL LETTER A WITH DIAERESIS
					break;
				case 0x00E5:		//	å	0xc3 0xa5	LATIN SMALL LETTER A WITH RING ABOVE
					*p = 0x00C5;	//	Å	0xc3 0x85	LATIN CAPITAL LETTER A WITH RING ABOVE
					break;
				case 0x00E6:		//	æ	0xc3 0xa6	LATIN SMALL LETTER AE
					*p = 0x00C6;	//	Æ	0xc3 0x86	LATIN CAPITAL LETTER AE
					break;
				case 0x00E7:		//	ç	0xc3 0xa7	LATIN SMALL LETTER C WITH CEDILLA
					*p = 0x00C7;	//	Ç	0xc3 0x87	LATIN CAPITAL LETTER C WITH CEDILLA
					break;
				case 0x00E8:		//	è	0xc3 0xa8	LATIN SMALL LETTER E WITH GRAVE
					*p = 0x00C8;	//	È	0xc3 0x88	LATIN CAPITAL LETTER E WITH GRAVE
					break;
				case 0x00E9:		//	é	0xc3 0xa9	LATIN SMALL LETTER E WITH ACUTE
					*p = 0x00C9;	//	É	0xc3 0x89	LATIN CAPITAL LETTER E WITH ACUTE
					break;
				case 0x00EA:		//	ê	0xc3 0xaa	LATIN SMALL LETTER E WITH CIRCUMFLEX
					*p = 0x00CA;	//	Ê	0xc3 0x8a	LATIN CAPITAL LETTER E WITH CIRCUMFLEX
					break;
				case 0x00EB:		//	ë	0xc3 0xab	LATIN SMALL LETTER E WITH DIAERESIS
					*p = 0x00CB;	//	Ë	0xc3 0x8b	LATIN CAPITAL LETTER E WITH DIAERESIS
					break;
				case 0x00EC:		//	ì	0xc3 0xac	LATIN SMALL LETTER I WITH GRAVE
					*p = 0x00CC;	//	Ì	0xc3 0x8c	LATIN CAPITAL LETTER I WITH GRAVE
					break;
				case 0x00ED:		//	í	0xc3 0xad	LATIN SMALL LETTER I WITH ACUTE
					*p = 0x00CD;	//	Í	0xc3 0x8d	LATIN CAPITAL LETTER I WITH ACUTE
					break;
				case 0x00EE:		//	î	0xc3 0xae	LATIN SMALL LETTER I WITH CIRCUMFLEX
					*p = 0x00CE;	//	Î	0xc3 0x8e	LATIN CAPITAL LETTER I WITH CIRCUMFLEX
					break;
				case 0x00EF:		//	ï	0xc3 0xaf	LATIN SMALL LETTER I WITH DIAERESIS
					*p = 0x00CF;	//	Ï	0xc3 0x8f	LATIN CAPITAL LETTER I WITH DIAERESIS
					break;
				case 0x00F0:		//	ð	0xc3 0xb0	LATIN SMALL LETTER ETH
					*p = 0x00D0;	//	Ð	0xc3 0x90	LATIN CAPITAL LETTER ETH
					break;
				case 0x00F1:		//	ñ	0xc3 0xb1	LATIN SMALL LETTER N WITH TILDE
					*p = 0x00D1;	//	Ñ	0xc3 0x91	LATIN CAPITAL LETTER N WITH TILDE
					break;
				case 0x00F2:		//	ò	0xc3 0xb2	LATIN SMALL LETTER O WITH GRAVE
					*p = 0x00D2;	//	Ò	0xc3 0x92	LATIN CAPITAL LETTER O WITH GRAVE
					break;
				case 0x00F3:		//	ó	0xc3 0xb3	LATIN SMALL LETTER O WITH ACUTE
					*p = 0x00D3;	//	Ó	0xc3 0x93	LATIN CAPITAL LETTER O WITH ACUTE
					break;
				case 0x00F4:		//	ô	0xc3 0xb4	LATIN SMALL LETTER O WITH CIRCUMFLEX
					*p = 0x00D4;	//	Ô	0xc3 0x94	LATIN CAPITAL LETTER O WITH CIRCUMFLEX
					break;
				case 0x00F5:		//	õ	0xc3 0xb5	LATIN SMALL LETTER O WITH TILDE
					*p = 0x00D5;	//	Õ	0xc3 0x95	LATIN CAPITAL LETTER O WITH TILDE
					break;
				case 0x00F6:		//	ö	0xc3 0xb6	LATIN SMALL LETTER O WITH DIAERESIS
					*p = 0x00D6;	//	Ö	0xc3 0x96	LATIN CAPITAL LETTER O WITH DIAERESIS
					break;
				case 0x00F8:		//	ø	0xc3 0xb8	LATIN SMALL LETTER O WITH STROKE
					*p = 0x00D8;	//	Ø	0xc3 0x98	LATIN CAPITAL LETTER O WITH STROKE
					break;
				case 0x00F9:		//	ù	0xc3 0xb9	LATIN SMALL LETTER U WITH GRAVE
					*p = 0x00D9;	//	Ù	0xc3 0x99	LATIN CAPITAL LETTER U WITH GRAVE
					break;
				case 0x00FA:		//	ú	0xc3 0xba	LATIN SMALL LETTER U WITH ACUTE
					*p = 0x00DA;	//	Ú	0xc3 0x9a	LATIN CAPITAL LETTER U WITH ACUTE
					break;
				case 0x00FB:		//	û	0xc3 0xbb	LATIN SMALL LETTER U WITH CIRCUMFLEX
					*p = 0x00DB;	//	Û	0xc3 0x9b	LATIN CAPITAL LETTER U WITH CIRCUMFLEX
					break;
				case 0x00FC:		//	ü	0xc3 0xbc	LATIN SMALL LETTER U WITH DIAERESIS
					*p = 0x00DC;	//	Ü	0xc3 0x9c	LATIN CAPITAL LETTER U WITH DIAERESIS
					break;
				case 0x00FD:		//	ý	0xc3 0xbd	LATIN SMALL LETTER Y WITH ACUTE
					*p = 0x00DD;	//	Ý	0xc3 0x9d	LATIN CAPITAL LETTER Y WITH ACUTE
					break;
				case 0x00FE:		//	þ	0xc3 0xbe	LATIN SMALL LETTER THORN
					*p = 0x00DE;	//	Þ	0xc3 0x9e	LATIN CAPITAL LETTER THORN
					break;
				case 0x00FF:		//	ÿ	0xc3 0xbf	LATIN SMALL LETTER Y WITH DIAERESIS
					*p = 0x0178;	//	Ÿ	0xc5 0xb8	LATIN CAPITAL LETTER Y WITH DIAERESIS
					break;
				case 0x0101:		//	ā	0xc4 0x81	LATIN SMALL LETTER A WITH MACRON
					*p = 0x0100;	//	Ā	0xc4 0x80	LATIN CAPITAL LETTER A WITH MACRON
					break;
				case 0x0103:		//	ă	0xc4 0x83	LATIN SMALL LETTER A WITH BREVE
					*p = 0x0102;	//	Ă	0xc4 0x82	LATIN CAPITAL LETTER A WITH BREVE
					break;
				case 0x0105:		//	ą	0xc4 0x85	LATIN SMALL LETTER A WITH OGONEK
					*p = 0x0104;	//	Ą	0xc4 0x84	LATIN CAPITAL LETTER A WITH OGONEK
					break;
				case 0x0107:		//	ć	0xc4 0x87	LATIN SMALL LETTER C WITH ACUTE
					*p = 0x0106;	//	Ć	0xc4 0x86	LATIN CAPITAL LETTER C WITH ACUTE
					break;
				case 0x0109:		//	ĉ	0xc4 0x89	LATIN SMALL LETTER C WITH CIRCUMFLEX
					*p = 0x0108;	//	Ĉ	0xc4 0x88	LATIN CAPITAL LETTER C WITH CIRCUMFLEX
					break;
				case 0x010B:		//	ċ	0xc4 0x8b	LATIN SMALL LETTER C WITH DOT ABOVE
					*p = 0x010A;	//	Ċ	0xc4 0x8a	LATIN CAPITAL LETTER C WITH DOT ABOVE
					break;
				case 0x010D:		//	č	0xc4 0x8d	LATIN SMALL LETTER C WITH CARON
					*p = 0x010C;	//	Č	0xc4 0x8c	LATIN CAPITAL LETTER C WITH CARON
					break;
				case 0x010F:		//	ď	0xc4 0x8f	LATIN SMALL LETTER D WITH CARON
					*p = 0x010E;	//	Ď	0xc4 0x8e	LATIN CAPITAL LETTER D WITH CARON
					break;
				case 0x0111:		//	đ	0xc4 0x91	LATIN SMALL LETTER D WITH STROKE
					*p = 0x0110;	//	Đ	0xc4 0x90	LATIN CAPITAL LETTER D WITH STROKE
					break;
				case 0x0113:		//	ē	0xc4 0x93	LATIN SMALL LETTER E WITH MACRON
					*p = 0x0112;	//	Ē	0xc4 0x92	LATIN CAPITAL LETTER E WITH MACRON
					break;
				case 0x0115:		//	ĕ	0xc4 0x95	LATIN SMALL LETTER E WITH BREVE
					*p = 0x0114;	//	Ĕ	0xc4 0x94	LATIN CAPITAL LETTER E WITH BREVE
					break;
				case 0x0117:		//	ė	0xc4 0x97	LATIN SMALL LETTER E WITH DOT ABOVE
					*p = 0x0116;	//	Ė	0xc4 0x96	LATIN CAPITAL LETTER E WITH DOT ABOVE
					break;
				case 0x0119:		//	ę	0xc4 0x99	LATIN SMALL LETTER E WITH OGONEK
					*p = 0x0118;	//	Ę	0xc4 0x98	LATIN CAPITAL LETTER E WITH OGONEK
					break;
				case 0x011B:		//	ě	0xc4 0x9b	LATIN SMALL LETTER E WITH CARON
					*p = 0x011A;	//	Ě	0xc4 0x9a	LATIN CAPITAL LETTER E WITH CARON
					break;
				case 0x011D:		//	ĝ	0xc4 0x9d	LATIN SMALL LETTER G WITH CIRCUMFLEX
					*p = 0x011C;	//	Ĝ	0xc4 0x9c	LATIN CAPITAL LETTER G WITH CIRCUMFLEX
					break;
				case 0x011F:		//	ğ	0xc4 0x9f	LATIN SMALL LETTER G WITH BREVE
					*p = 0x011E;	//	Ğ	0xc4 0x9e	LATIN CAPITAL LETTER G WITH BREVE
					break;
				case 0x0121:		//	ġ	0xc4 0xa1	LATIN SMALL LETTER G WITH DOT ABOVE
					*p = 0x0120;	//	Ġ	0xc4 0xa0	LATIN CAPITAL LETTER G WITH DOT ABOVE
					break;
				case 0x0123:		//	ģ	0xc4 0xa3	LATIN SMALL LETTER G WITH CEDILLA
					*p = 0x0122;	//	Ģ	0xc4 0xa2	LATIN CAPITAL LETTER G WITH CEDILLA
					break;
				case 0x0125:		//	ĥ	0xc4 0xa5	LATIN SMALL LETTER H WITH CIRCUMFLEX
					*p = 0x0124;	//	Ĥ	0xc4 0xa4	LATIN CAPITAL LETTER H WITH CIRCUMFLEX
					break;
				case 0x0127:		//	ħ	0xc4 0xa7	LATIN SMALL LETTER H WITH STROKE
					*p = 0x0126;	//	Ħ	0xc4 0xa6	LATIN CAPITAL LETTER H WITH STROKE
					break;
				case 0x0129:		//	ĩ	0xc4 0xa9	LATIN SMALL LETTER I WITH TILDE
					*p = 0x0128;	//	Ĩ	0xc4 0xa8	LATIN CAPITAL LETTER I WITH TILDE
					break;
				case 0x012B:		//	ī	0xc4 0xab	LATIN SMALL LETTER I WITH MACRON
					*p = 0x012A;	//	Ī	0xc4 0xaa	LATIN CAPITAL LETTER I WITH MACRON
					break;
				case 0x012D:		//	ĭ	0xc4 0xad	LATIN SMALL LETTER I WITH BREVE
					*p = 0x012C;	//	Ĭ	0xc4 0xac	LATIN CAPITAL LETTER I WITH BREVE
					break;
				case 0x012F:		//	į	0xc4 0xaf	LATIN SMALL LETTER I WITH OGONEK
					*p = 0x012E;	//	Į	0xc4 0xae	LATIN CAPITAL LETTER I WITH OGONEK
					break;
				case 0x0131:		//	ı	0xc4 0xb1	LATIN SMALL LETTER DOTLESS I
					*p = 0x0049;	//	I	0x49 LATIN CAPITAL LETTER I
					break;
				case 0x0133:		//	ĳ	0xc4 0xb3	LATIN SMALL LETTER LIGATURE IJ
					*p = 0x0132;	//	Ĳ	0xc4 0xb2	LATIN CAPITAL LETTER LIGATURE IJ
					break;
				case 0x0135:		//	ĵ	0xc4 0xb5	LATIN SMALL LETTER J WITH CIRCUMFLEX
					*p = 0x0134;	//	Ĵ	0xc4 0xb4	LATIN CAPITAL LETTER J WITH CIRCUMFLEX
					break;
				case 0x0137:		//	ķ	0xc4 0xb7	LATIN SMALL LETTER K WITH CEDILLA
					*p = 0x0136;	//	Ķ	0xc4 0xb6	LATIN CAPITAL LETTER K WITH CEDILLA
					break;
				case 0x013A:		//	ĺ	0xc4 0xba	LATIN SMALL LETTER L WITH ACUTE
					*p = 0x0139;	//	Ĺ	0xc4 0xb9	LATIN CAPITAL LETTER L WITH ACUTE
					break;
				case 0x013C:		//	ļ	0xc4 0xbc	LATIN SMALL LETTER L WITH CEDILLA
					*p = 0x013B;	//	Ļ	0xc4 0xbb	LATIN CAPITAL LETTER L WITH CEDILLA
					break;
				case 0x013E:		//	ľ	0xc4 0xbe	LATIN SMALL LETTER L WITH CARON
					*p = 0x013D;	//	Ľ	0xc4 0xbd	LATIN CAPITAL LETTER L WITH CARON
					break;
				case 0x0140:		//	ŀ	0xc5 0x80	LATIN SMALL LETTER L WITH MIDDLE DOT
					*p = 0x013F;	//	Ŀ	0xc4 0xbf	LATIN CAPITAL LETTER L WITH MIDDLE DOT
					break;
				case 0x0142:		//	ł	0xc5 0x82	LATIN SMALL LETTER L WITH STROKE
					*p = 0x0141;	//	Ł	0xc5 0x81	LATIN CAPITAL LETTER L WITH STROKE
					break;
				case 0x0144:		//	ń	0xc5 0x84	LATIN SMALL LETTER N WITH ACUTE
					*p = 0x0143;	//	Ń	0xc5 0x83	LATIN CAPITAL LETTER N WITH ACUTE
					break;
				case 0x0146:		//	ņ	0xc5 0x86	LATIN SMALL LETTER N WITH CEDILLA
					*p = 0x0145;	//	Ņ	0xc5 0x85	LATIN CAPITAL LETTER N WITH CEDILLA
					break;
				case 0x0148:		//	ň	0xc5 0x88	LATIN SMALL LETTER N WITH CARON
					*p = 0x0147;	//	Ň	0xc5 0x87	LATIN CAPITAL LETTER N WITH CARON
					break;
				case 0x014B:		//	ŋ	0xc5 0x8b	LATIN SMALL LETTER ENG
					*p = 0x014A;	//	Ŋ	0xc5 0x8a	LATIN CAPITAL LETTER ENG
					break;
				case 0x014D:		//	ō	0xc5 0x8d	LATIN SMALL LETTER O WITH MACRON
					*p = 0x014C;	//	Ō	0xc5 0x8c	LATIN CAPITAL LETTER O WITH MACRON
					break;
				case 0x014F:		//	ŏ	0xc5 0x8f	LATIN SMALL LETTER O WITH BREVE
					*p = 0x014E;	//	Ŏ	0xc5 0x8e	LATIN CAPITAL LETTER O WITH BREVE
					break;
				case 0x0151:		//	ő	0xc5 0x91	LATIN SMALL LETTER O WITH DOUBLE ACUTE
					*p = 0x0150;	//	Ő	0xc5 0x90	LATIN CAPITAL LETTER O WITH DOUBLE ACUTE
					break;
				case 0x0153:		//	œ	0xc5 0x93	LATIN SMALL LETTER LIGATURE OE
					*p = 0x0152;	//	Œ	0xc5 0x92	LATIN CAPITAL LETTER LIGATURE OE
					break;
				case 0x0155:		//	ŕ	0xc5 0x95	LATIN SMALL LETTER R WITH ACUTE
					*p = 0x0154;	//	Ŕ	0xc5 0x94	LATIN CAPITAL LETTER R WITH ACUTE
					break;
				case 0x0157:		//	ŗ	0xc5 0x97	LATIN SMALL LETTER R WITH CEDILLA
					*p = 0x0156;	//	Ŗ	0xc5 0x96	LATIN CAPITAL LETTER R WITH CEDILLA
					break;
				case 0x0159:		//	ř	0xc5 0x99	LATIN SMALL LETTER R WITH CARON
					*p = 0x0158;	//	Ř	0xc5 0x98	LATIN CAPITAL LETTER R WITH CARON
					break;
				case 0x015B:		//	ś	0xc5 0x9b	LATIN SMALL LETTER S WITH ACUTE
					*p = 0x015A;	//	Ś	0xc5 0x9a	LATIN CAPITAL LETTER S WITH ACUTE
					break;
				case 0x015D:		//	ŝ	0xc5 0x9d	LATIN SMALL LETTER S WITH CIRCUMFLEX
					*p = 0x015C;	//	Ŝ	0xc5 0x9c	LATIN CAPITAL LETTER S WITH CIRCUMFLEX
					break;
				case 0x015F:		//	ş	0xc5 0x9f	LATIN SMALL LETTER S WITH CEDILLA
					*p = 0x015E;	//	Ş	0xc5 0x9e	LATIN CAPITAL LETTER S WITH CEDILLA
					break;
				case 0x0161:		//	š	0xc5 0xa1	LATIN SMALL LETTER S WITH CARON
					*p = 0x0160;	//	Š	0xc5 0xa0	LATIN CAPITAL LETTER S WITH CARON
					break;
				case 0x0163:		//	ţ	0xc5 0xa3	LATIN SMALL LETTER T WITH CEDILLA
					*p = 0x0162;	//	Ţ	0xc5 0xa2	LATIN CAPITAL LETTER T WITH CEDILLA
					break;
				case 0x0165:		//	ť	0xc5 0xa5	LATIN SMALL LETTER T WITH CARON
					*p = 0x0164;	//	Ť	0xc5 0xa4	LATIN CAPITAL LETTER T WITH CARON
					break;
				case 0x0167:		//	ŧ	0xc5 0xa7	LATIN SMALL LETTER T WITH STROKE
					*p = 0x0166;	//	Ŧ	0xc5 0xa6	LATIN CAPITAL LETTER T WITH STROKE
					break;
				case 0x0169:		//	ũ	0xc5 0xa9	LATIN SMALL LETTER U WITH TILDE
					*p = 0x0168;	//	Ũ	0xc5 0xa8	LATIN CAPITAL LETTER U WITH TILDE
					break;
				case 0x016B:		//	ū	0xc5 0xab	LATIN SMALL LETTER U WITH MACRON
					*p = 0x016A;	//	Ū	0xc5 0xaa	LATIN CAPITAL LETTER U WITH MACRON
					break;
				case 0x016D:		//	ŭ	0xc5 0xad	LATIN SMALL LETTER U WITH BREVE
					*p = 0x016C;	//	Ŭ	0xc5 0xac	LATIN CAPITAL LETTER U WITH BREVE
					break;
				case 0x016F:		//	ů	0xc5 0xaf	LATIN SMALL LETTER U WITH RING ABOVE
					*p = 0x016E;	//	Ů	0xc5 0xae	LATIN CAPITAL LETTER U WITH RING ABOVE
					break;
				case 0x0171:		//	ű	0xc5 0xb1	LATIN SMALL LETTER U WITH DOUBLE ACUTE
					*p = 0x0170;	//	Ű	0xc5 0xb0	LATIN CAPITAL LETTER U WITH DOUBLE ACUTE
					break;
				case 0x0173:		//	ų	0xc5 0xb3	LATIN SMALL LETTER U WITH OGONEK
					*p = 0x0172;	//	Ų	0xc5 0xb2	LATIN CAPITAL LETTER U WITH OGONEK
					break;
				case 0x0175:		//	ŵ	0xc5 0xb5	LATIN SMALL LETTER W WITH CIRCUMFLEX
					*p = 0x0174;	//	Ŵ	0xc5 0xb4	LATIN CAPITAL LETTER W WITH CIRCUMFLEX
					break;
				case 0x0177:		//	ŷ	0xc5 0xb7	LATIN SMALL LETTER Y WITH CIRCUMFLEX
					*p = 0x0176;	//	Ŷ	0xc5 0xb6	LATIN CAPITAL LETTER Y WITH CIRCUMFLEX
					break;
				case 0x017A:		//	ź	0xc5 0xba	LATIN SMALL LETTER Z WITH ACUTE
					*p = 0x0179;	//	Ź	0xc5 0xb9	LATIN CAPITAL LETTER Z WITH ACUTE
					break;
				case 0x017C:		//	ż	0xc5 0xbc	LATIN SMALL LETTER Z WITH DOT ABOVE
					*p = 0x017B;	//	Ż	0xc5 0xbb	LATIN CAPITAL LETTER Z WITH DOT ABOVE
					break;
				case 0x017E:		//	ž	0xc5 0xbe	LATIN SMALL LETTER Z WITH CARON
					*p = 0x017D;	//	Ž	0xc5 0xbd	LATIN CAPITAL LETTER Z WITH CARON
					break;
				case 0x0180:		//	ƀ	0xc6 0x80	LATIN SMALL LETTER B WITH STROKE
					*p = 0x0243;	//	Ƀ	0xc9 0x83	LATIN CAPITAL LETTER B WITH STROKE
					break;
				case 0x0183:		//	ƃ	0xc6 0x83	LATIN SMALL LETTER B WITH TOPBAR
					*p = 0x0182;	//	Ƃ	0xc6 0x82	LATIN CAPITAL LETTER B WITH TOPBAR
					break;
				case 0x0185:		//	ƅ	0xc6 0x85	LATIN SMALL LETTER TONE SIX
					*p = 0x0184;	//	Ƅ	0xc6 0x84	LATIN CAPITAL LETTER TONE SIX
					break;
				case 0x0188:		//	ƈ	0xc6 0x88	LATIN SMALL LETTER C WITH HOOK
					*p = 0x0187;	//	Ƈ	0xc6 0x87	LATIN CAPITAL LETTER C WITH HOOK
					break;
				case 0x018C:		//	ƌ	0xc6 0x8c	LATIN SMALL LETTER D WITH TOPBAR
					*p = 0x018B;	//	Ƌ	0xc6 0x8b	LATIN CAPITAL LETTER D WITH TOPBAR
					break;
				case 0x0192:		//	ƒ	0xc6 0x92	LATIN SMALL LETTER F WITH HOOK
					*p = 0x0191;	//	Ƒ	0xc6 0x91	LATIN CAPITAL LETTER F WITH HOOK
					break;
				case 0x0195:		//	ƕ	0xc6 0x95	LATIN SMALL LETTER HWAIR
					*p = 0x01F6;	//	Ƕ	0xc7 0xb6	LATIN CAPITAL LETTER HWAIR
					break;
				case 0x0199:		//	ƙ	0xc6 0x99	LATIN SMALL LETTER K WITH HOOK
					*p = 0x0198;	//	Ƙ	0xc6 0x98	LATIN CAPITAL LETTER K WITH HOOK
					break;
				case 0x019A:		//	ƚ	0xc6 0x9a	LATIN SMALL LETTER L WITH BAR
					*p = 0x023D;	//	Ƚ	0xc8 0xbd	LATIN CAPITAL LETTER L WITH BAR
					break;
				case 0x019E:		//	ƞ	0xc6 0x9e	LATIN SMALL LETTER N WITH LONG RIGHT LEG
					*p = 0x0220;	//	Ƞ	0xc8 0xa0	LATIN CAPITAL LETTER N WITH LONG RIGHT LEG
					break;
				case 0x01A1:		//	ơ	0xc6 0xa1	LATIN SMALL LETTER O WITH HORN
					*p = 0x01A0;	//	Ơ	0xc6 0xa0	LATIN CAPITAL LETTER O WITH HORN
					break;
				case 0x01A3:		//	ƣ	0xc6 0xa3	LATIN SMALL LETTER OI
					*p = 0x01A2;	//	Ƣ	0xc6 0xa2	LATIN CAPITAL LETTER OI
					break;
				case 0x01A5:		//	ƥ	0xc6 0xa5	LATIN SMALL LETTER P WITH HOOK
					*p = 0x01A4;	//	Ƥ	0xc6 0xa4	LATIN CAPITAL LETTER P WITH HOOK
					break;
				case 0x01A8:		//	ƨ	0xc6 0xa8	LATIN SMALL LETTER TONE TWO
					*p = 0x01A7;	//	Ƨ	0xc6 0xa7	LATIN CAPITAL LETTER TONE TWO
					break;
				case 0x01AD:		//	ƭ	0xc6 0xad	LATIN SMALL LETTER T WITH HOOK
					*p = 0x01AC;	//	Ƭ	0xc6 0xac	LATIN CAPITAL LETTER T WITH HOOK
					break;
				case 0x01B0:		//	ư	0xc6 0xb0	LATIN SMALL LETTER U WITH HORN
					*p = 0x01AF;	//	Ư	0xc6 0xaf	LATIN CAPITAL LETTER U WITH HORN
					break;
				case 0x01B4:		//	ƴ	0xc6 0xb4	LATIN SMALL LETTER Y WITH HOOK
					*p = 0x01B3;	//	Ƴ	0xc6 0xb3	LATIN CAPITAL LETTER Y WITH HOOK
					break;
				case 0x01B6:		//	ƶ	0xc6 0xb6	LATIN SMALL LETTER Z WITH STROKE
					*p = 0x01B5;	//	Ƶ	0xc6 0xb5	LATIN CAPITAL LETTER Z WITH STROKE
					break;
				case 0x01B9:		//	ƹ	0xc6 0xb9	LATIN SMALL LETTER EZH REVERSED
					*p = 0x01B8;	//	Ƹ	0xc6 0xb8	LATIN CAPITAL LETTER EZH REVERSED
					break;
				case 0x01BD:		//	ƽ	0xc6 0xbd	LATIN SMALL LETTER TONE FIVE
					*p = 0x01BC;	//	Ƽ	0xc6 0xbc	LATIN CAPITAL LETTER TONE FIVE
					break;
				case 0x01BF:		//	ƿ	0xc6 0xbf	LATIN SMALL LETTER WYNN
					*p = 0x01F7;	//	Ƿ	0xc7 0xb7	LATIN CAPITAL LETTER WYNN
					break;
				case 0x01C5:		//	ǅ	0xc7 0x85	LATIN CAPITAL LETTER D WITH SMALL Z WITH CARON
				case 0x01C6:		//	ǆ	0xc7 0x86	LATIN SMALL LETTER DZ WITH CARON
					*p = 0x01C4;	//	Ǆ	0xc7 0x84	LATIN CAPITAL LETTER DZ WITH CARON
					break;
				case 0x01C8:		//	ǈ	0xc7 0x88	LATIN CAPITAL LETTER L WITH SMALL J
				case 0x01C9:		//	ǉ	0xc7 0x89	LATIN SMALL LETTER LJ
					*p = 0x01C7;	//	Ǉ	0xc7 0x87	LATIN CAPITAL LETTER LJ
					break;
				case 0x01CB:		//	ǋ	0xc7 0x8b	LATIN CAPITAL LETTER N WITH SMALL J
				case 0x01CC:		//	ǌ	0xc7 0x8c	LATIN SMALL LETTER NJ
					*p = 0x01CA;	//	Ǌ	0xc7 0x8a	LATIN CAPITAL LETTER NJ
					break;
				case 0x01CE:		//	ǎ	0xc7 0x8e	LATIN SMALL LETTER A WITH CARON
					*p = 0x01CD;	//	Ǎ	0xc7 0x8d	LATIN CAPITAL LETTER A WITH CARON
					break;
				case 0x01D0:		//	ǐ	0xc7 0x90	LATIN SMALL LETTER I WITH CARON
					*p = 0x01CF;	//	Ǐ	0xc7 0x8f	LATIN CAPITAL LETTER I WITH CARON
					break;
				case 0x01D2:		//	ǒ	0xc7 0x92	LATIN SMALL LETTER O WITH CARON
					*p = 0x01D1;	//	Ǒ	0xc7 0x91	LATIN CAPITAL LETTER O WITH CARON
					break;
				case 0x01D4:		//	ǔ	0xc7 0x94	LATIN SMALL LETTER U WITH CARON
					*p = 0x01D3;	//	Ǔ	0xc7 0x93	LATIN CAPITAL LETTER U WITH CARON
					break;
				case 0x01D6:		//	ǖ	0xc7 0x96	LATIN SMALL LETTER U WITH DIAERESIS AND MACRON
					*p = 0x01D5;	//	Ǖ	0xc7 0x95	LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON
					break;
				case 0x01D8:		//	ǘ	0xc7 0x98	LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE
					*p = 0x01D7;	//	Ǘ	0xc7 0x97	LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE
					break;
				case 0x01DA:		//	ǚ	0xc7 0x9a	LATIN SMALL LETTER U WITH DIAERESIS AND CARON
					*p = 0x01D9;	//	Ǚ	0xc7 0x99	LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON
					break;
				case 0x01DC:		//	ǜ	0xc7 0x9c	LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE
					*p = 0x01DB;	//	Ǜ	0xc7 0x9b	LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE
					break;
				case 0x01DD:		//	ǝ	0xC7 0x9D	LATIN SMALL LETTER TURNED E (look mid page 297 https://www.unicode.org/versions/Unicode14.0.0/ch07.pdf)
					*p = 0x018E;	//	Ǝ	0xc6 0x8e	LATIN CAPITAL LETTER REVERSED E
					break;
				case 0x01DF:		//	ǟ	0xc7 0x9f	LATIN SMALL LETTER A WITH DIAERESIS AND MACRON
					*p = 0x01DE;	//	Ǟ	0xc7 0x9e	LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON
					break;
				case 0x01E1:		//	ǡ	0xc7 0xa1	LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON
					*p = 0x01E0;	//	Ǡ	0xc7 0xa0	LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON
					break;
				case 0x01E3:		//	ǣ	0xc7 0xa3	LATIN SMALL LETTER AE WITH MACRON
					*p = 0x01E2;	//	Ǣ	0xc7 0xa2	LATIN CAPITAL LETTER AE WITH MACRON
					break;
				case 0x01E5:		//	ǥ	0xc7 0xa5	LATIN SMALL LETTER G WITH STROKE
					*p = 0x01E4;	//	Ǥ	0xc7 0xa4	LATIN CAPITAL LETTER G WITH STROKE
					break;
				case 0x01E7:		//	ǧ	0xc7 0xa7	LATIN SMALL LETTER G WITH CARON
					*p = 0x01E6;	//	Ǧ	0xc7 0xa6	LATIN CAPITAL LETTER G WITH CARON
					break;
				case 0x01E9:		//	ǩ	0xc7 0xa9	LATIN SMALL LETTER K WITH CARON
					*p = 0x01E8;	//	Ǩ	0xc7 0xa8	LATIN CAPITAL LETTER K WITH CARON
					break;
				case 0x01EB:		//	ǫ	0xc7 0xab	LATIN SMALL LETTER O WITH OGONEK
					*p = 0x01EA;	//	Ǫ	0xc7 0xaa	LATIN CAPITAL LETTER O WITH OGONEK
					break;
				case 0x01ED:		//	ǭ	0xc7 0xad	LATIN SMALL LETTER O WITH OGONEK AND MACRON
					*p = 0x01EC;	//	Ǭ	0xc7 0xac	LATIN CAPITAL LETTER O WITH OGONEK AND MACRON
					break;
				case 0x01EF:		//	ǯ	0xc7 0xaf	LATIN SMALL LETTER EZH WITH CARON
					*p = 0x01EE;	//	Ǯ	0xc7 0xae	LATIN CAPITAL LETTER EZH WITH CARON
					break;
				case 0x01F2:		//	ǲ	0xc7 0xb2	LATIN CAPITAL LETTER D WITH SMALL Z
				case 0x01F3:		//	ǳ	0xc7 0xb3	LATIN SMALL LETTER DZ
					*p = 0x01F1;	//	Ǳ	0xc7 0xb1	LATIN CAPITAL LETTER DZ
					break;
				case 0x01F5:		//	ǵ	0xc7 0xb5	LATIN SMALL LETTER G WITH ACUTE
					*p = 0x01F4;	//	Ǵ	0xc7 0xb4	LATIN CAPITAL LETTER G WITH ACUTE
					break;
				case 0x01F9:		//	ǹ	0xc7 0xb9	LATIN SMALL LETTER N WITH GRAVE
					*p = 0x01F8;	//	Ǹ	0xc7 0xb8	LATIN CAPITAL LETTER N WITH GRAVE
					break;
				case 0x01FB:		//	ǻ	0xc7 0xbb	LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE
					*p = 0x01FA;	//	Ǻ	0xc7 0xba	LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE
					break;
				case 0x01FD:		//	ǽ	0xc7 0xbd	LATIN SMALL LETTER AE WITH ACUTE
					*p = 0x01FC;	//	Ǽ	0xc7 0xbc	LATIN CAPITAL LETTER AE WITH ACUTE
					break;
				case 0x01FF:		//	ǿ	0xc7 0xbf	LATIN SMALL LETTER O WITH STROKE AND ACUTE
					*p = 0x01FE;	//	Ǿ	0xc7 0xbe	LATIN CAPITAL LETTER O WITH STROKE AND ACUTE
					break;
				case 0x0201:		//	ȁ	0xc8 0x81	LATIN SMALL LETTER A WITH DOUBLE GRAVE
					*p = 0x0200;	//	Ȁ	0xc8 0x80	LATIN CAPITAL LETTER A WITH DOUBLE GRAVE
					break;
				case 0x0203:		//	ȃ	0xc8 0x83	LATIN SMALL LETTER A WITH INVERTED BREVE
					*p = 0x0202;	//	Ȃ	0xc8 0x82	LATIN CAPITAL LETTER A WITH INVERTED BREVE
					break;
				case 0x0205:		//	ȅ	0xc8 0x85	LATIN SMALL LETTER E WITH DOUBLE GRAVE
					*p = 0x0204;	//	Ȅ	0xc8 0x84	LATIN CAPITAL LETTER E WITH DOUBLE GRAVE
					break;
				case 0x0207:		//	ȇ	0xc8 0x87	LATIN SMALL LETTER E WITH INVERTED BREVE
					*p = 0x0206;	//	Ȇ	0xc8 0x86	LATIN CAPITAL LETTER E WITH INVERTED BREVE
					break;
				case 0x0209:		//	ȉ	0xc8 0x89	LATIN SMALL LETTER I WITH DOUBLE GRAVE
					*p = 0x0208;	//	Ȉ	0xc8 0x88	LATIN CAPITAL LETTER I WITH DOUBLE GRAVE
					break;
				case 0x020B:		//	ȋ	0xc8 0x8b	LATIN SMALL LETTER I WITH INVERTED BREVE
					*p = 0x020A;	//	Ȋ	0xc8 0x8a	LATIN CAPITAL LETTER I WITH INVERTED BREVE
					break;
				case 0x020D:		//	ȍ	0xc8 0x8d	LATIN SMALL LETTER O WITH DOUBLE GRAVE
					*p = 0x020C;	//	Ȍ	0xc8 0x8c	LATIN CAPITAL LETTER O WITH DOUBLE GRAVE
					break;
				case 0x020F:		//	ȏ	0xc8 0x8f	LATIN SMALL LETTER O WITH INVERTED BREVE
					*p = 0x020E;	//	Ȏ	0xc8 0x8e	LATIN CAPITAL LETTER O WITH INVERTED BREVE
					break;
				case 0x0211:		//	ȑ	0xc8 0x91	LATIN SMALL LETTER R WITH DOUBLE GRAVE
					*p = 0x0210;	//	Ȑ	0xc8 0x90	LATIN CAPITAL LETTER R WITH DOUBLE GRAVE
					break;
				case 0x0213:		//	ȓ	0xc8 0x93	LATIN SMALL LETTER R WITH INVERTED BREVE
					*p = 0x0212;	//	Ȓ	0xc8 0x92	LATIN CAPITAL LETTER R WITH INVERTED BREVE
					break;
				case 0x0215:		//	ȕ	0xc8 0x95	LATIN SMALL LETTER U WITH DOUBLE GRAVE
					*p = 0x0214;	//	Ȕ	0xc8 0x94	LATIN CAPITAL LETTER U WITH DOUBLE GRAVE
					break;
				case 0x0217:		//	ȗ	0xc8 0x97	LATIN SMALL LETTER U WITH INVERTED BREVE
					*p = 0x0216;	//	Ȗ	0xc8 0x96	LATIN CAPITAL LETTER U WITH INVERTED BREVE
					break;
				case 0x0219:		//	ș	0xc8 0x99	LATIN SMALL LETTER S WITH COMMA BELOW
					*p = 0x0218;	//	Ș	0xc8 0x98	LATIN CAPITAL LETTER S WITH COMMA BELOW
					break;
				case 0x021B:		//	ț	0xc8 0x9b	LATIN SMALL LETTER T WITH COMMA BELOW
					*p = 0x021A;	//	Ț	0xc8 0x9a	LATIN CAPITAL LETTER T WITH COMMA BELOW
					break;
				case 0x021D:		//	ȝ	0xc8 0x9d	LATIN SMALL LETTER YOGH
					*p = 0x021C;	//	Ȝ	0xc8 0x9c	LATIN CAPITAL LETTER YOGH
					break;
				case 0x021F:		//	ȟ	0xc8 0x9f	LATIN SMALL LETTER H WITH CARON
					*p = 0x021E;	//	Ȟ	0xc8 0x9e	LATIN CAPITAL LETTER H WITH CARON
					break;
				case 0x0223:		//	ȣ	0xc8 0xa3	LATIN SMALL LETTER OU
					*p = 0x0222;	//	Ȣ	0xc8 0xa2	LATIN CAPITAL LETTER OU
					break;
				case 0x0225:		//	ȥ	0xc8 0xa5	LATIN SMALL LETTER Z WITH HOOK
					*p = 0x0224;	//	Ȥ	0xc8 0xa4	LATIN CAPITAL LETTER Z WITH HOOK
					break;
				case 0x0227:		//	ȧ	0xc8 0xa7	LATIN SMALL LETTER A WITH DOT ABOVE
					*p = 0x0226;	//	Ȧ	0xc8 0xa6	LATIN CAPITAL LETTER A WITH DOT ABOVE
					break;
				case 0x0229:		//	ȩ	0xc8 0xa9	LATIN SMALL LETTER E WITH CEDILLA
					*p = 0x0228;	//	Ȩ	0xc8 0xa8	LATIN CAPITAL LETTER E WITH CEDILLA
					break;
				case 0x022B:		//	ȫ	0xc8 0xab	LATIN SMALL LETTER O WITH DIAERESIS AND MACRON
					*p = 0x022A;	//	Ȫ	0xc8 0xaa	LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON
					break;
				case 0x022D:		//	ȭ	0xc8 0xad	LATIN SMALL LETTER O WITH TILDE AND MACRON
					*p = 0x022C;	//	Ȭ	0xc8 0xac	LATIN CAPITAL LETTER O WITH TILDE AND MACRON
					break;
				case 0x022F:		//	ȯ	0xc8 0xaf	LATIN SMALL LETTER O WITH DOT ABOVE
					*p = 0x022E;	//	Ȯ	0xc8 0xae	LATIN CAPITAL LETTER O WITH DOT ABOVE
					break;
				case 0x0231:		//	ȱ	0xc8 0xb1	LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON
					*p = 0x0230;	//	Ȱ	0xc8 0xb0	LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON
					break;
				case 0x0233:		//	ȳ	0xc8 0xb3	LATIN SMALL LETTER Y WITH MACRON
					*p = 0x0232;	//	Ȳ	0xc8 0xb2	LATIN CAPITAL LETTER Y WITH MACRON
					break;
				case 0x023C:		//	ȼ	0xc8 0xbc	LATIN SMALL LETTER C WITH STROKE
					*p = 0x023B;	//	Ȼ	0xc8 0xbb	LATIN CAPITAL LETTER C WITH STROKE
					break;
				case 0x023F:		//	ȿ	0xc8 0xbf	LATIN SMALL LETTER S WITH SWASH TAIL
					*p = 0x2C7E;	//	Ȿ	0xe2 0xb1 0xbe	LATIN CAPITAL LETTER S WITH SWASH TAIL
					break;
				case 0x0240:		//	ɀ	0xc9 0x80	LATIN SMALL LETTER Z WITH SWASH TAIL
					*p = 0x2C7F;	//	Ɀ	0xe2 0xb1 0xbf	LATIN CAPITAL LETTER Z WITH SWASH TAIL
					break;
				case 0x0242:		//	ɂ	0xc9 0x82	LATIN SMALL LETTER GLOTTAL STOP
					*p = 0x0241;	//	Ɂ	0xc9 0x81	LATIN CAPITAL LETTER GLOTTAL STOP
					break;
				case 0x0247:		//	ɇ	0xc9 0x87	LATIN SMALL LETTER E WITH STROKE
					*p = 0x0246;	//	Ɇ	0xc9 0x86	LATIN CAPITAL LETTER E WITH STROKE
					break;
				case 0x0249:		//	ɉ	0xc9 0x89	LATIN SMALL LETTER J WITH STROKE
					*p = 0x0248;	//	Ɉ	0xc9 0x88	LATIN CAPITAL LETTER J WITH STROKE
					break;
				case 0x024B:		//	ɋ	0xc9 0x8b	LATIN SMALL LETTER SMALL Q WITH HOOK TAIL
					*p = 0x024A;	//	Ɋ	0xc9 0x8a	LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL
					break;
				case 0x024D:		//	ɍ	0xc9 0x8d	LATIN SMALL LETTER R WITH STROKE
					*p = 0x024C;	//	Ɍ	0xc9 0x8c	LATIN CAPITAL LETTER R WITH STROKE
					break;
				case 0x024F:		//	ɏ	0xc9 0x8f	LATIN SMALL LETTER Y WITH STROKE
					*p = 0x024E;	//	Ɏ	0xc9 0x8e	LATIN CAPITAL LETTER Y WITH STROKE
					break;
				case 0x0250:		//	ɐ	0xc9 0x90	LATIN SMALL LETTER TURNED A
					*p = 0x2C6F;	//	Ɐ	0xe2 0xb1 0xaf	LATIN CAPITAL LETTER TURNED A
					break;
				case 0x0251:		//	ɑ	0xc9 0x91	LATIN SMALL LETTER ALPHA
					*p = 0x2C6D;	//	Ɑ	0xe2 0xb1 0xad	LATIN CAPITAL LETTER ALPHA
					break;
				case 0x0252:		//	ɒ	0xc9 0x92	LATIN SMALL LETTER TURNED ALPHA
					*p = 0x2C70;	//	Ɒ	0xe2 0xb1 0xb0	LATIN CAPITAL LETTER TURNED ALPHA
					break;
				case 0x0253:		//	ɓ	0xc9 0x93	LATIN SMALL LETTER B WITH HOOK
					*p = 0x0181;	//	Ɓ	0xc6 0x81	LATIN CAPITAL LETTER B WITH HOOK
					break;
				case 0x0254:		//	ɔ	0xc9 0x94	LATIN SMALL LETTER OPEN O
					*p = 0x0186;	//	Ɔ	0xc6 0x86	LATIN CAPITAL LETTER OPEN O
					break;
				case 0x0256:		//	ɖ	0xc9 0x96	LATIN SMALL LETTER AFRICAN D
					*p = 0x0189;	//	Ɖ	0xc6 0x89	LATIN CAPITAL LETTER AFRICAN D
					break;
				case 0x0257:		//	ɗ	0xc9 0x97	LATIN SMALL LETTER D WITH HOOK
					*p = 0x018A;	//	Ɗ	0xc6 0x8a	LATIN CAPITAL LETTER D WITH HOOK
					break;
				case 0x0259:		//	ə	0xc9 0x99	LATIN SMALL LETTER SCHWA
					*p = 0x018F;	//	Ə	0xc6 0x8f	LATIN CAPITAL LETTER SCHWA
					break;
				case 0x025B:		//	ɛ	0xc9 0x9b	LATIN SMALL LETTER OPEN E
					*p = 0x0190;	//	Ɛ	0xc6 0x90	LATIN CAPITAL LETTER OPEN E
					break;
				case 0x025C:		//	ɜ	0xc9 0x9c	LATIN SMALL LETTER REVERSED OPEN E
					*p = 0xA7AB;	//	Ɜ	0xea 0x9e 0xab	LATIN CAPITAL LETTER REVERSED OPEN E
					break;
				case 0x0260:		//	ɠ	0xc9 0xa0	LATIN SMALL LETTER G WITH HOOK
					*p = 0x0193;	//	Ɠ	0xc6 0x93	LATIN CAPITAL LETTER G WITH HOOK
					break;
				case 0x0261:		//	ɡ	0xc9 0xa1	LATIN SMALL LETTER SCRIPT G
					*p = 0xA7AC;	//	Ɡ	0xea 0x9e 0xac	LATIN CAPITAL LETTER SCRIPT G
					break;
				case 0x0263:		//	ɣ	0xc9 0xa3	LATIN SMALL LETTER GAMMA
					*p = 0x0194;	//	Ɣ	0xc6 0x94	LATIN CAPITAL LETTER GAMMA
					break;
				case 0x0265:		//	ɥ	0xc9 0xa5	LATIN SMALL LETTER TURNED H
					*p = 0xA78D;	//	Ɥ	0xea 0x9e 0x8d	LATIN CAPITAL LETTER TURNED H
					break;
				case 0x0266:		//	ɦ	0xc9 0xa6	LATIN SMALL LETTER H WITH HOOK
					*p = 0xA7AA;	//	Ɦ	0xea 0x9e 0xaa	LATIN CAPITAL LETTER H WITH HOOK
					break;
				case 0x0268:		//	ɨ	0xc9 0xa8	LATIN SMALL LETTER I WITH STROKE
					*p = 0x0197;	//	Ɨ	0xc6 0x97	LATIN CAPITAL LETTER I WITH STROKE
					break;
				case 0x0269:		//	ɩ	0xc9 0xa9	LATIN SMALL LETTER IOTA
					*p = 0x0196;	//	Ɩ	0xc6 0x96	LATIN CAPITAL LETTER IOTA
					break;
				case 0x026A:		//	ɪ	0xc9 0xaa	LATIN SMALL LETTER SMALL CAPITAL I
					*p = 0xA7AE;	//	Ɪ	0xea 0x9e 0xae	LATIN CAPITAL LETTER SMALL CAPITAL I
					break;
				case 0x026B:		//	ɫ	0xc9 0xab	LATIN SMALL LETTER L WITH MIDDLE TILDE
					*p = 0x2C62;	//	Ɫ	0xe2 0xb1 0xa2	LATIN CAPITAL LETTER L WITH MIDDLE TILDE
					break;
				case 0x026C:		//	ɬ	0xc9 0xac	LATIN SMALL LETTER L WITH BELT
					*p = 0xA7AD;	//	Ɬ	0xea 0x9e 0xad	LATIN CAPITAL LETTER L WITH BELT
					break;
				case 0x026F:		//	ɯ	0xc9 0xaf	LATIN SMALL LETTER TURNED M
					*p = 0x019C;	//	Ɯ	0xc6 0x9c	LATIN CAPITAL LETTER TURNED M
					break;
				case 0x0271:		//	ɱ	0xc9 0xb1	LATIN SMALL LETTER M WITH HOOK
					*p = 0x2C6E;	//	Ɱ	0xe2 0xb1 0xae	LATIN CAPITAL LETTER M WITH HOOK
					break;
				case 0x0272:		//	ɲ	0xc9 0xb2	LATIN SMALL LETTER N WITH LEFT HOOK
					*p = 0x019D;	//	Ɲ	0xc6 0x9d	LATIN CAPITAL LETTER N WITH LEFT HOOK
					break;
				case 0x0275:		//	ɵ	0xc9 0xb5	LATIN SMALL LETTER O WITH MIDDLE TILDE
					*p = 0x019F;	//	Ɵ	0xc6 0x9f	LATIN CAPITAL LETTER O WITH MIDDLE TILDE
					break;
				case 0x027D:		//	ɽ	0xc9 0xbd	LATIN SMALL LETTER R WITH TAIL
					*p = 0x2C64;	//	Ɽ	0xe2 0xb1 0xa4	LATIN CAPITAL LETTER R WITH TAIL
					break;
				case 0x0282:		//	ʂ	0xca 0x82	LATIN SMALL LETTER S WITH HOOK
					*p = 0xA7C5;	//	Ʂ	0xea 0x9f 0x85	LATIN CAPITAL LETTER S WITH HOOK
					break;
				case 0x0283:		//	ʃ	0xca 0x83	LATIN SMALL LETTER ESH
					*p = 0x01A9;	//	Ʃ	0xc6 0xa9	LATIN CAPITAL LETTER ESH
					break;
				case 0x0287:		//	ʇ	0xca 0x87	LATIN SMALL LETTER TURNED T
					*p = 0xA7B1;	//	Ʇ	0xea 0x9e 0xb1	LATIN CAPITAL LETTER TURNED T
					break;
				case 0x0288:		//	ʈ	0xca 0x88	LATIN SMALL LETTER T WITH RETROFLEX HOOK
					*p = 0x01AE;	//	Ʈ	0xc6 0xae	LATIN CAPITAL LETTER T WITH RETROFLEX HOOK
					break;
				case 0x0289:		//	ʉ	0xca 0x89	LATIN SMALL LETTER U BAR
					*p = 0x0244;	//	Ʉ	0xc9 0x84	LATIN CAPITAL LETTER U BAR
					break;
				case 0x028A:		//	ʊ	0xca 0x8a	LATIN SMALL LETTER UPSILON
					*p = 0x01B1;	//	Ʊ	0xc6 0xb1	LATIN CAPITAL LETTER UPSILON
					break;
				case 0x028B:		//	ʋ	0xca 0x8b	LATIN SMALL LETTER V WITH HOOK
					*p = 0x01B2;	//	Ʋ	0xc6 0xb2	LATIN CAPITAL LETTER V WITH HOOK
					break;
				case 0x028C:		//	ʌ	0xca 0x8c	LATIN SMALL LETTER TURNED V
					*p = 0x0245;	//	Ʌ	0xc9 0x85	LATIN CAPITAL LETTER TURNED V
					break;
				case 0x0292:		//	ʒ	0xca 0x92	LATIN SMALL LETTER EZH
					*p = 0x01B7;	//	Ʒ	0xc6 0xb7	LATIN CAPITAL LETTER EZH
					break;
				case 0x029D:		//	ʝ	0xca 0x9d	LATIN SMALL LETTER J WITH CROSSED-TAIL
					*p = 0xA7B2;	//	Ʝ	0xea 0x9e 0xb2	LATIN CAPITAL LETTER J WITH CROSSED-TAIL
					break;
				case 0x029E:		//	ʞ	0xca 0x9e	LATIN SMALL LETTER TURNED K
					*p = 0xA7B0;	//	Ʞ	0xea 0x9e 0xb0	LATIN CAPITAL LETTER TURNED K
					break;
				case 0x0371:		//	ͱ	0xcd 0xb1	GREEK SMALL LETTER HETA
					*p = 0x0370;	//	Ͱ	0xcd 0xb0	GREEK CAPITAL LETTER HETA
					break;
				case 0x0373:		//	ͳ	0xcd 0xb3	GREEK SMALL LETTER ARCHAIC SAMPI
					*p = 0x0372;	//	Ͳ	0xcd 0xb2	GREEK CAPITAL LETTER ARCHAIC SAMPI
					break;
				case 0x0377:		//	ͷ	0xcd 0xb7	GREEK SMALL LETTER PAMPHYLIAN DIGAMMA
					*p = 0x0376;	//	Ͷ	0xcd 0xb6	GREEK CAPITAL LETTER PAMPHYLIAN DIGAMMA
					break;
				case 0x037B:		//	ͻ	0xcd 0xbb	GREEK SMALL LETTER REVERSED LUNATE SIGMA SYMBOL
					*p = 0x03FD;	//	Ͻ	0xcf 0xbd	GREEK CAPITAL LETTER REVERSED LUNATE SIGMA SYMBOL
					break;
				case 0x037C:		//	ͼ	0xcd 0xbc	GREEK SMALL LETTER DOTTED LUNATE SIGMA SYMBOL
					*p = 0x03FE;	//	Ͼ	0xcf 0xbe	GREEK CAPITAL LETTER DOTTED LUNATE SIGMA SYMBOL
					break;
				case 0x037D:		//	ͽ	0xcd 0xbd	GREEK SMALL LETTER REVERSED DOTTED LUNATE SIGMA SYMBOL
					*p = 0x03FF;	//	Ͽ	0xcf 0xbf	GREEK CAPITAL LETTER REVERSED DOTTED LUNATE SIGMA SYMBOL
					break;
				case 0x03AC:		//	ά	0xce 0xac	GREEK SMALL LETTER ALPHA WITH TONOS
					*p = 0x0386;	//	Ά	0xce 0x86	GREEK CAPITAL LETTER ALPHA WITH TONOS
					break;
				case 0x03AD:		//	έ	0xce 0xad	GREEK SMALL LETTER EPSILON WITH TONOS
					*p = 0x0388;	//	Έ	0xce 0x88	GREEK CAPITAL LETTER EPSILON WITH TONOS
					break;
				case 0x03AE:		//	ή	0xce 0xae	GREEK SMALL LETTER ETA WITH TONOS
					*p = 0x0389;	//	Ή	0xce 0x89	GREEK CAPITAL LETTER ETA WITH TONOS
					break;
				case 0x03AF:		//	ί	0xce 0xaf	GREEK SMALL LETTER IOTA WITH TONOS
					*p = 0x038A;	//	Ί	0xce 0x8a	GREEK CAPITAL LETTER IOTA WITH TONOS
					break;
				case 0x03B1:		//	α	0xce 0xb1	GREEK SMALL LETTER ALPHA
					*p = 0x0391;	//	Α	0xce 0x91	GREEK CAPITAL LETTER ALPHA
					break;
				case 0x03B2:		//	β	0xce 0xb2	GREEK SMALL LETTER BETA
					*p = 0x0392;	//	Β	0xce 0x92	GREEK CAPITAL LETTER BETA
					break;
				case 0x03B3:		//	γ	0xce 0xb3	GREEK SMALL LETTER GAMMA
					*p = 0x0393;	//	Γ	0xce 0x93	GREEK CAPITAL LETTER GAMMA
					break;
				case 0x03B4:		//	δ	0xce 0xb4	GREEK SMALL LETTER DELTA
					*p = 0x0394;	//	Δ	0xce 0x94	GREEK CAPITAL LETTER DELTA
					break;
				case 0x03B5:		//	ε	0xce 0xb5	GREEK SMALL LETTER EPSILON
					*p = 0x0395;	//	Ε	0xce 0x95	GREEK CAPITAL LETTER EPSILON
					break;
				case 0x03B6:		//	ζ	0xce 0xb6	GREEK SMALL LETTER ZETA
					*p = 0x0396;	//	Ζ	0xce 0x96	GREEK CAPITAL LETTER ZETA
					break;
				case 0x03B7:		//	η	0xce 0xb7	GREEK SMALL LETTER ETA
					*p = 0x0397;	//	Η	0xce 0x97	GREEK CAPITAL LETTER ETA
					break;
				case 0x03B8:		//	θ	0xce 0xb8	GREEK SMALL LETTER THETA
					*p = 0x0398;	//	Θ	0xce 0x98	GREEK CAPITAL LETTER THETA
					break;
				case 0x03B9:		//	ι	0xce 0xb9	GREEK SMALL LETTER IOTA
					*p = 0x0399;	//	Ι	0xce 0x99	GREEK CAPITAL LETTER IOTA
					break;
				case 0x03BA:		//	κ	0xce 0xba	GREEK SMALL LETTER KAPPA
					*p = 0x039A;	//	Κ	0xce 0x9a	GREEK CAPITAL LETTER KAPPA
					break;
				case 0x03BB:		//	λ	0xce 0xbb	GREEK SMALL LETTER LAMDA
					*p = 0x039B;	//	Λ	0xce 0x9b	GREEK CAPITAL LETTER LAMDA
					break;
				case 0x03BC:		//	μ	0xce 0xbc	GREEK SMALL LETTER MU
					*p = 0x039C;	//	Μ	0xce 0x9c	GREEK CAPITAL LETTER MU
					break;
				case 0x03BD:		//	ν	0xce 0xbd	GREEK SMALL LETTER NU
					*p = 0x039D;	//	Ν	0xce 0x9d	GREEK CAPITAL LETTER NU
					break;
				case 0x03BE:		//	ξ	0xce 0xbe	GREEK SMALL LETTER XI
					*p = 0x039E;	//	Ξ	0xce 0x9e	GREEK CAPITAL LETTER XI
					break;
				case 0x03BF:		//	ο	0xce 0xbf	GREEK SMALL LETTER OMICRON
					*p = 0x039F;	//	Ο	0xce 0x9f	GREEK CAPITAL LETTER OMICRON
					break;
				case 0x03C0:		//	π	0xcf 0x80	GREEK SMALL LETTER PI
					*p = 0x03A0;	//	Π	0xce 0xa0	GREEK CAPITAL LETTER PI
					break;
				case 0x03C1:		//	ρ	0xcf 0x81	GREEK SMALL LETTER RHO
					*p = 0x03A1;	//	Ρ	0xce 0xa1	GREEK CAPITAL LETTER RHO
					break;
				case 0x03C2:		//	ς	0xcf 0x82	GREEK SMALL FINAL SIGMA
				case 0x03C3:		//	σ	0xcf 0x83	GREEK SMALL LETTER SIGMA
					*p = 0x03A3;	//	Σ	0xce 0xa3	GREEK CAPITAL LETTER SIGMA
					break;
				case 0x03C4:		//	τ	0xcf 0x84	GREEK SMALL LETTER TAU
					*p = 0x03A4;	//	Τ	0xce 0xa4	GREEK CAPITAL LETTER TAU
					break;
				case 0x03C5:		//	υ	0xcf 0x85	GREEK SMALL LETTER UPSILON
					*p = 0x03A5;	//	Υ	0xce 0xa5	GREEK CAPITAL LETTER UPSILON
					break;
				case 0x03C6:		//	φ	0xcf 0x86	GREEK SMALL LETTER PHI
					*p = 0x03A6;	//	Φ	0xce 0xa6	GREEK CAPITAL LETTER PHI
					break;
				case 0x03C7:		//	χ	0xcf 0x87	GREEK SMALL LETTER CHI
					*p = 0x03A7;	//	Χ	0xce 0xa7	GREEK CAPITAL LETTER CHI
					break;
				case 0x03C8:		//	ψ	0xcf 0x88	GREEK SMALL LETTER PSI
					*p = 0x03A8;	//	Ψ	0xce 0xa8	GREEK CAPITAL LETTER PSI
					break;
				case 0x03C9:		//	ω	0xcf 0x89	GREEK SMALL LETTER OMEGA
					*p = 0x03A9;	//	Ω	0xce 0xa9	GREEK CAPITAL LETTER OMEGA
					break;
				case 0x03CA:		//	ϊ	0xcf 0x8a	GREEK SMALL LETTER IOTA WITH DIALYTIKA
					*p = 0x03AA;	//	Ϊ	0xce 0xaa	GREEK CAPITAL LETTER IOTA WITH DIALYTIKA
					break;
				case 0x03CB:		//	ϋ	0xcf 0x8b	GREEK SMALL LETTER UPSILON WITH DIALYTIKA
					*p = 0x03AB;	//	Ϋ	0xce 0xab	GREEK CAPITAL LETTER UPSILON WITH DIALYTIKA
					break;
				case 0x03CC:		//	ό	0xcf 0x8c	GREEK SMALL LETTER OMICRON WITH TONOS
					*p = 0x038C;	//	Ό	0xce 0x8c	GREEK CAPITAL LETTER OMICRON WITH TONOS
					break;
				case 0x03CD:		//	ύ	0xcf 0x8d	GREEK SMALL LETTER UPSILON WITH TONOS
					*p = 0x038E;	//	Ύ	0xce 0x8e	GREEK CAPITAL LETTER UPSILON WITH TONOS
					break;
				case 0x03CE:		//	ώ	0xcf 0x8e	GREEK SMALL LETTER OMEGA WITH TONOS
					*p = 0x038F;	//	Ώ	0xce 0x8f	GREEK CAPITAL LETTER OMEGA WITH TONOS
					break;
				case 0x03D1:		//	ϑ	0xcf 0x91	GREEK SMALL LETTER THETA SYMBOL
					*p = 0x03F4;	//	ϴ	0xcf 0xb4	GREEK CAPITAL LETTER THETA SYMBOL
					break;
				case 0x03D7:		//	ϗ	0xcf 0x97	GREEK SMALL LETTER KAI SYMBOL
					*p = 0x03CF;	//	Ϗ	0xcf 0x8f	GREEK CAPITAL LETTER KAI SYMBOL
					break;
				case 0x03D9:		//	ϙ	0xcf 0x99	GREEK SMALL LETTER ARCHAIC KOPPA
					*p = 0x03D8;	//	Ϙ	0xcf 0x98	GREEK CAPITAL LETTER ARCHAIC KOPPA
					break;
				case 0x03DB:		//	ϛ	0xcf 0x9b	GREEK SMALL LETTER STIGMA
					*p = 0x03DA;	//	Ϛ	0xcf 0x9a	GREEK CAPITAL LETTER STIGMA
					break;
				case 0x03DD:		//	ϝ	0xcf 0x9d	GREEK SMALL LETTER DIGAMMA
					*p = 0x03DC;	//	Ϝ	0xcf 0x9c	GREEK CAPITAL LETTER DIGAMMA
					break;
				case 0x03DF:		//	ϟ	0xcf 0x9f	GREEK SMALL LETTER KOPPA
					*p = 0x03DE;	//	Ϟ	0xcf 0x9e	GREEK CAPITAL LETTER KOPPA
					break;
				case 0x03E1:		//	ϡ	0xcf 0xa1	GREEK SMALL LETTER SAMPI
					*p = 0x03E0;	//	Ϡ	0xcf 0xa0	GREEK CAPITAL LETTER SAMPI
					break;
				case 0x03E3:		//	ϣ	0xcf 0xa3	COPTIC SMALL LETTER SHEI
					*p = 0x03E2;	//	Ϣ	0xcf 0xa2	COPTIC CAPITAL LETTER SHEI
					break;
				case 0x03E5:		//	ϥ	0xcf 0xa5	COPTIC SMALL LETTER FEI
					*p = 0x03E4;	//	Ϥ	0xcf 0xa4	COPTIC CAPITAL LETTER FEI
					break;
				case 0x03E7:		//	ϧ	0xcf 0xa7	COPTIC SMALL LETTER KHEI
					*p = 0x03E6;	//	Ϧ	0xcf 0xa6	COPTIC CAPITAL LETTER KHEI
					break;
				case 0x03E9:		//	ϩ	0xcf 0xa9	COPTIC SMALL LETTER HORI
					*p = 0x03E8;	//	Ϩ	0xcf 0xa8	COPTIC CAPITAL LETTER HORI
					break;
				case 0x03EB:		//	ϫ	0xcf 0xab	COPTIC SMALL LETTER GANGIA
					*p = 0x03EA;	//	Ϫ	0xcf 0xaa	COPTIC CAPITAL LETTER GANGIA
					break;
				case 0x03ED:		//	ϭ	0xcf 0xad	COPTIC SMALL LETTER SHIMA
					*p = 0x03EC;	//	Ϭ	0xcf 0xac	COPTIC CAPITAL LETTER SHIMA
					break;
				case 0x03EF:		//	ϯ	0xcf 0xaf	COPTIC SMALL LETTER DEI
					*p = 0x03EE;	//	Ϯ	0xcf 0xae	COPTIC CAPITAL LETTER DEI
					break;
				case 0x03F2:		//	ϲ	0xcf 0xb2	GREEK SMALL LETTER LUNATE SIGMA SYMBOL
					*p = 0x03F9;	//	Ϲ	0xcf 0xb9	GREEK CAPITAL LETTER LUNATE SIGMA SYMBOL
					break;
				case 0x03F3:		//	ϳ	0xcf 0xb3	GREEK SMALL LETTER YOT
					*p = 0x037F;	//	Ϳ	0xcd 0xbf	GREEK CAPITAL LETTER YOT
					break;
				case 0x03F8:		//	ϸ	0xcf 0xb8	GREEK SMALL LETTER SHO
					*p = 0x03F7;	//	Ϸ	0xcf 0xb7	GREEK CAPITAL LETTER SHO
					break;
				case 0x03FB:		//	ϻ	0xcf 0xbb	GREEK SMALL LETTER SAN
					*p = 0x03FA;	//	Ϻ	0xcf 0xba	GREEK CAPITAL LETTER SAN
					break;
				case 0x0430:		//	а	0xd0 0xb0	CYRILLIC SMALL LETTER A
					*p = 0x0410;	//	А	0xd0 0x90	CYRILLIC CAPITAL LETTER A
					break;
				case 0x0431:		//	б	0xd0 0xb1	CYRILLIC SMALL LETTER BE
					*p = 0x0411;	//	Б	0xd0 0x91	CYRILLIC CAPITAL LETTER BE
					break;
				case 0x0432:		//	в	0xd0 0xb2	CYRILLIC SMALL LETTER VE
					*p = 0x0412;	//	В	0xd0 0x92	CYRILLIC CAPITAL LETTER VE
					break;
				case 0x0433:		//	г	0xd0 0xb3	CYRILLIC SMALL LETTER GHE
					*p = 0x0413;	//	Г	0xd0 0x93	CYRILLIC CAPITAL LETTER GHE
					break;
				case 0x0434:		//	д	0xd0 0xb4	CYRILLIC SMALL LETTER DE
					*p = 0x0414;	//	Д	0xd0 0x94	CYRILLIC CAPITAL LETTER DE
					break;
				case 0x0435:		//	е	0xd0 0xb5	CYRILLIC SMALL LETTER IE
					*p = 0x0415;	//	Е	0xd0 0x95	CYRILLIC CAPITAL LETTER IE
					break;
				case 0x0436:		//	ж	0xd0 0xb6	CYRILLIC SMALL LETTER ZHE
					*p = 0x0416;	//	Ж	0xd0 0x96	CYRILLIC CAPITAL LETTER ZHE
					break;
				case 0x0437:		//	з	0xd0 0xb7	CYRILLIC SMALL LETTER ZE
					*p = 0x0417;	//	З	0xd0 0x97	CYRILLIC CAPITAL LETTER ZE
					break;
				case 0x0438:		//	и	0xd0 0xb8	CYRILLIC SMALL LETTER I
					*p = 0x0418;	//	И	0xd0 0x98	CYRILLIC CAPITAL LETTER I
					break;
				case 0x0439:		//	й	0xd0 0xb9	CYRILLIC SMALL LETTER SHORT I
					*p = 0x0419;	//	Й	0xd0 0x99	CYRILLIC CAPITAL LETTER SHORT I
					break;
				case 0x043A:		//	к	0xd0 0xba	CYRILLIC SMALL LETTER KA
					*p = 0x041A;	//	К	0xd0 0x9a	CYRILLIC CAPITAL LETTER KA
					break;
				case 0x043B:		//	л	0xd0 0xbb	CYRILLIC SMALL LETTER EL
					*p = 0x041B;	//	Л	0xd0 0x9b	CYRILLIC CAPITAL LETTER EL
					break;
				case 0x043C:		//	м	0xd0 0xbc	CYRILLIC SMALL LETTER EM
					*p = 0x041C;	//	М	0xd0 0x9c	CYRILLIC CAPITAL LETTER EM
					break;
				case 0x043D:		//	н	0xd0 0xbd	CYRILLIC SMALL LETTER EN
					*p = 0x041D;	//	Н	0xd0 0x9d	CYRILLIC CAPITAL LETTER EN
					break;
				case 0x043E:		//	о	0xd0 0xbe	CYRILLIC SMALL LETTER O
					*p = 0x041E;	//	О	0xd0 0x9e	CYRILLIC CAPITAL LETTER O
					break;
				case 0x043F:		//	п	0xd0 0xbf	CYRILLIC SMALL LETTER PE
					*p = 0x041F;	//	П	0xd0 0x9f	CYRILLIC CAPITAL LETTER PE
					break;
				case 0x0440:		//	р	0xd1 0x80	CYRILLIC SMALL LETTER ER
					*p = 0x0420;	//	Р	0xd0 0xa0	CYRILLIC CAPITAL LETTER ER
					break;
				case 0x0441:		//	с	0xd1 0x81	CYRILLIC SMALL LETTER ES
					*p = 0x0421;	//	С	0xd0 0xa1	CYRILLIC CAPITAL LETTER ES
					break;
				case 0x0442:		//	т	0xd1 0x82	CYRILLIC SMALL LETTER TE
					*p = 0x0422;	//	Т	0xd0 0xa2	CYRILLIC CAPITAL LETTER TE
					break;
				case 0x0443:		//	у	0xd1 0x83	CYRILLIC SMALL LETTER U
					*p = 0x0423;	//	У	0xd0 0xa3	CYRILLIC CAPITAL LETTER U
					break;
				case 0x0444:		//	ф	0xd1 0x84	CYRILLIC SMALL LETTER EF
					*p = 0x0424;	//	Ф	0xd0 0xa4	CYRILLIC CAPITAL LETTER EF
					break;
				case 0x0445:		//	х	0xd1 0x85	CYRILLIC SMALL LETTER HA
					*p = 0x0425;	//	Х	0xd0 0xa5	CYRILLIC CAPITAL LETTER HA
					break;
				case 0x0446:		//	ц	0xd1 0x86	CYRILLIC SMALL LETTER TSE
					*p = 0x0426;	//	Ц	0xd0 0xa6	CYRILLIC CAPITAL LETTER TSE
					break;
				case 0x0447:		//	ч	0xd1 0x87	CYRILLIC SMALL LETTER CHE
					*p = 0x0427;	//	Ч	0xd0 0xa7	CYRILLIC CAPITAL LETTER CHE
					break;
				case 0x0448:		//	ш	0xd1 0x88	CYRILLIC SMALL LETTER SHA
					*p = 0x0428;	//	Ш	0xd0 0xa8	CYRILLIC CAPITAL LETTER SHA
					break;
				case 0x0449:		//	щ	0xd1 0x89	CYRILLIC SMALL LETTER SHCHA
					*p = 0x0429;	//	Щ	0xd0 0xa9	CYRILLIC CAPITAL LETTER SHCHA
					break;
				case 0x044A:		//	ъ	0xd1 0x8a	CYRILLIC SMALL LETTER HARD SIGN
					*p = 0x042A;	//	Ъ	0xd0 0xaa	CYRILLIC CAPITAL LETTER HARD SIGN
					break;
				case 0x044B:		//	ы	0xd1 0x8b	CYRILLIC SMALL LETTER YERU
					*p = 0x042B;	//	Ы	0xd0 0xab	CYRILLIC CAPITAL LETTER YERU
					break;
				case 0x044C:		//	ь	0xd1 0x8c	CYRILLIC SMALL LETTER SOFT SIGN
					*p = 0x042C;	//	Ь	0xd0 0xac	CYRILLIC CAPITAL LETTER SOFT SIGN
					break;
				case 0x044D:		//	э	0xd1 0x8d	CYRILLIC SMALL LETTER E
					*p = 0x042D;	//	Э	0xd0 0xad	CYRILLIC CAPITAL LETTER E
					break;
				case 0x044E:		//	ю	0xd1 0x8e	CYRILLIC SMALL LETTER YU
					*p = 0x042E;	//	Ю	0xd0 0xae	CYRILLIC CAPITAL LETTER YU
					break;
				case 0x044F:		//	я	0xd1 0x8f	CYRILLIC SMALL LETTER YA
					*p = 0x042F;	//	Я	0xd0 0xaf	CYRILLIC CAPITAL LETTER YA
					break;
				case 0x0450:		//	ѐ	0xd1 0x90	CYRILLIC SMALL LETTER IE WITH GRAVE
					*p = 0x0400;	//	Ѐ	0xd0 0x80	CYRILLIC CAPITAL LETTER IE WITH GRAVE
					break;
				case 0x0451:		//	ё	0xd1 0x91	CYRILLIC SMALL LETTER IO
					*p = 0x0401;	//	Ё	0xd0 0x81	CYRILLIC CAPITAL LETTER IO
					break;
				case 0x0452:		//	ђ	0xd1 0x92	CYRILLIC SMALL LETTER DJE
					*p = 0x0402;	//	Ђ	0xd0 0x82	CYRILLIC CAPITAL LETTER DJE
					break;
				case 0x0453:		//	ѓ	0xd1 0x93	CYRILLIC SMALL LETTER GJE
					*p = 0x0403;	//	Ѓ	0xd0 0x83	CYRILLIC CAPITAL LETTER GJE
					break;
				case 0x0454:		//	є	0xd1 0x94	CYRILLIC SMALL LETTER UKRAINIAN IE
					*p = 0x0404;	//	Є	0xd0 0x84	CYRILLIC CAPITAL LETTER UKRAINIAN IE
					break;
				case 0x0455:		//	ѕ	0xd1 0x95	CYRILLIC SMALL LETTER DZE
					*p = 0x0405;	//	Ѕ	0xd0 0x85	CYRILLIC CAPITAL LETTER DZE
					break;
				case 0x0456:		//	і	0xd1 0x96	CYRILLIC SMALL LETTER BYELORUSSIAN-UKRAINIAN I
					*p = 0x0406;	//	І	0xd0 0x86	CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I
					break;
				case 0x0457:		//	ї	0xd1 0x97	CYRILLIC SMALL LETTER YI
					*p = 0x0407;	//	Ї	0xd0 0x87	CYRILLIC CAPITAL LETTER YI
					break;
				case 0x0458:		//	ј	0xd1 0x98	CYRILLIC SMALL LETTER JE
					*p = 0x0408;	//	Ј	0xd0 0x88	CYRILLIC CAPITAL LETTER JE
					break;
				case 0x0459:		//	љ	0xd1 0x99	CYRILLIC SMALL LETTER LJE
					*p = 0x0409;	//	Љ	0xd0 0x89	CYRILLIC CAPITAL LETTER LJE
					break;
				case 0x045A:		//	њ	0xd1 0x9a	CYRILLIC SMALL LETTER NJE
					*p = 0x040A;	//	Њ	0xd0 0x8a	CYRILLIC CAPITAL LETTER NJE
					break;
				case 0x045B:		//	ћ	0xd1 0x9b	CYRILLIC SMALL LETTER TSHE
					*p = 0x040B;	//	Ћ	0xd0 0x8b	CYRILLIC CAPITAL LETTER TSHE
					break;
				case 0x045C:		//	ќ	0xd1 0x9c	CYRILLIC SMALL LETTER KJE
					*p = 0x040C;	//	Ќ	0xd0 0x8c	CYRILLIC CAPITAL LETTER KJE
					break;
				case 0x045D:		//	ѝ	0xd1 0x9d	CYRILLIC SMALL LETTER I WITH GRAVE
					*p = 0x040D;	//	Ѝ	0xd0 0x8d	CYRILLIC CAPITAL LETTER I WITH GRAVE
					break;
				case 0x045E:		//	ў	0xd1 0x9e	CYRILLIC SMALL LETTER SHORT U
					*p = 0x040E;	//	Ў	0xd0 0x8e	CYRILLIC CAPITAL LETTER SHORT U
					break;
				case 0x045F:		//	џ	0xd1 0x9f	CYRILLIC SMALL LETTER DZHE
					*p = 0x040F;	//	Џ	0xd0 0x8f	CYRILLIC CAPITAL LETTER DZHE
					break;
				case 0x0461:		//	ѡ	0xd1 0xa1	CYRILLIC SMALL LETTER OMEGA
					*p = 0x0460;	//	Ѡ	0xd1 0xa0	CYRILLIC CAPITAL LETTER OMEGA
					break;
				case 0x0463:		//	ѣ	0xd1 0xa3	CYRILLIC SMALL LETTER YAT
					*p = 0x0462;	//	Ѣ	0xd1 0xa2	CYRILLIC CAPITAL LETTER YAT
					break;
				case 0x0465:		//	ѥ	0xd1 0xa5	CYRILLIC SMALL LETTER IOTIFIED E
					*p = 0x0464;	//	Ѥ	0xd1 0xa4	CYRILLIC CAPITAL LETTER IOTIFIED E
					break;
				case 0x0467:		//	ѧ	0xd1 0xa7	CYRILLIC SMALL LETTER LITTLE YUS
					*p = 0x0466;	//	Ѧ	0xd1 0xa6	CYRILLIC CAPITAL LETTER LITTLE YUS
					break;
				case 0x0469:		//	ѩ	0xd1 0xa9	CYRILLIC SMALL LETTER IOTIFIED LITTLE YUS
					*p = 0x0468;	//	Ѩ	0xd1 0xa8	CYRILLIC CAPITAL LETTER IOTIFIED LITTLE YUS
					break;
				case 0x046B:		//	ѫ	0xd1 0xab	CYRILLIC SMALL LETTER BIG YUS
					*p = 0x046A;	//	Ѫ	0xd1 0xaa	CYRILLIC CAPITAL LETTER BIG YUS
					break;
				case 0x046D:		//	ѭ	0xd1 0xad	CYRILLIC SMALL LETTER IOTIFIED BIG YUS
					*p = 0x046C;	//	Ѭ	0xd1 0xac	CYRILLIC CAPITAL LETTER IOTIFIED BIG YUS
					break;
				case 0x046F:		//	ѯ	0xd1 0xaf	CYRILLIC SMALL LETTER KSI
					*p = 0x046E;	//	Ѯ	0xd1 0xae	CYRILLIC CAPITAL LETTER KSI
					break;
				case 0x0471:		//	ѱ	0xd1 0xb1	CYRILLIC SMALL LETTER PSI
					*p = 0x0470;	//	Ѱ	0xd1 0xb0	CYRILLIC CAPITAL LETTER PSI
					break;
				case 0x0473:		//	ѳ	0xd1 0xb3	CYRILLIC SMALL LETTER FITA
					*p = 0x0472;	//	Ѳ	0xd1 0xb2	CYRILLIC CAPITAL LETTER FITA
					break;
				case 0x0475:		//	ѵ	0xd1 0xb5	CYRILLIC SMALL LETTER IZHITSA
					*p = 0x0474;	//	Ѵ	0xd1 0xb4	CYRILLIC CAPITAL LETTER IZHITSA
					break;
				case 0x0477:		//	ѷ	0xd1 0xb7	CYRILLIC SMALL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT
					*p = 0x0476;	//	Ѷ	0xd1 0xb6	CYRILLIC CAPITAL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT
					break;
				case 0x0479:		//	ѹ	0xd1 0xb9	CYRILLIC SMALL LETTER UK
					*p = 0x0478;	//	Ѹ	0xd1 0xb8	CYRILLIC CAPITAL LETTER UK
					break;
				case 0x047B:		//	ѻ	0xd1 0xbb	CYRILLIC SMALL LETTER ROUND OMEGA
					*p = 0x047A;	//	Ѻ	0xd1 0xba	CYRILLIC CAPITAL LETTER ROUND OMEGA
					break;
				case 0x047D:		//	ѽ	0xd1 0xbd	CYRILLIC SMALL LETTER OMEGA WITH TITLO
					*p = 0x047C;	//	Ѽ	0xd1 0xbc	CYRILLIC CAPITAL LETTER OMEGA WITH TITLO
					break;
				case 0x047F:		//	ѿ	0xd1 0xbf	CYRILLIC SMALL LETTER OT
					*p = 0x047E;	//	Ѿ	0xd1 0xbe	CYRILLIC CAPITAL LETTER OT
					break;
				case 0x0481:		//	ҁ	0xd2 0x81	CYRILLIC SMALL LETTER KOPPA
					*p = 0x0480;	//	Ҁ	0xd2 0x80	CYRILLIC CAPITAL LETTER KOPPA
					break;
				case 0x048B:		//	ҋ	0xd2 0x8b	CYRILLIC SMALL LETTER SHORT I WITH TAIL
					*p = 0x048A;	//	Ҋ	0xd2 0x8a	CYRILLIC CAPITAL LETTER SHORT I WITH TAIL
					break;
				case 0x048D:		//	ҍ	0xd2 0x8d	CYRILLIC SMALL LETTER SEMISOFT SIGN
					*p = 0x048C;	//	Ҍ	0xd2 0x8c	CYRILLIC CAPITAL LETTER SEMISOFT SIGN
					break;
				case 0x048F:		//	ҏ	0xd2 0x8f	CYRILLIC SMALL LETTER ER WITH TICK
					*p = 0x048E;	//	Ҏ	0xd2 0x8e	CYRILLIC CAPITAL LETTER ER WITH TICK
					break;
				case 0x0491:		//	ґ	0xd2 0x91	CYRILLIC SMALL LETTER GHE WITH UPTURN
					*p = 0x0490;	//	Ґ	0xd2 0x90	CYRILLIC CAPITAL LETTER GHE WITH UPTURN
					break;
				case 0x0493:		//	ғ	0xd2 0x93	CYRILLIC SMALL LETTER GHE WITH STROKE
					*p = 0x0492;	//	Ғ	0xd2 0x92	CYRILLIC CAPITAL LETTER GHE WITH STROKE
					break;
				case 0x0495:		//	ҕ	0xd2 0x95	CYRILLIC SMALL LETTER GHE WITH MIDDLE HOOK
					*p = 0x0494;	//	Ҕ	0xd2 0x94	CYRILLIC CAPITAL LETTER GHE WITH MIDDLE HOOK
					break;
				case 0x0497:		//	җ	0xd2 0x97	CYRILLIC SMALL LETTER ZHE WITH DESCENDER
					*p = 0x0496;	//	Җ	0xd2 0x96	CYRILLIC CAPITAL LETTER ZHE WITH DESCENDER
					break;
				case 0x0499:		//	ҙ	0xd2 0x99	CYRILLIC SMALL LETTER ZE WITH DESCENDER
					*p = 0x0498;	//	Ҙ	0xd2 0x98	CYRILLIC CAPITAL LETTER ZE WITH DESCENDER
					break;
				case 0x049B:		//	қ	0xd2 0x9b	CYRILLIC SMALL LETTER KA WITH DESCENDER
					*p = 0x049A;	//	Қ	0xd2 0x9a	CYRILLIC CAPITAL LETTER KA WITH DESCENDER
					break;
				case 0x049D:		//	ҝ	0xd2 0x9d	CYRILLIC SMALL LETTER KA WITH VERTICAL STROKE
					*p = 0x049C;	//	Ҝ	0xd2 0x9c	CYRILLIC CAPITAL LETTER KA WITH VERTICAL STROKE
					break;
				case 0x049F:		//	ҟ	0xd2 0x9f	CYRILLIC SMALL LETTER KA WITH STROKE
					*p = 0x049E;	//	Ҟ	0xd2 0x9e	CYRILLIC CAPITAL LETTER KA WITH STROKE
					break;
				case 0x04A1:		//	ҡ	0xd2 0xa1	CYRILLIC SMALL LETTER BASHKIR KA
					*p = 0x04A0;	//	Ҡ	0xd2 0xa0	CYRILLIC CAPITAL LETTER BASHKIR KA
					break;
				case 0x04A3:		//	ң	0xd2 0xa3	CYRILLIC SMALL LETTER EN WITH DESCENDER
					*p = 0x04A2;	//	Ң	0xd2 0xa2	CYRILLIC CAPITAL LETTER EN WITH DESCENDER
					break;
				case 0x04A5:		//	ҥ	0xd2 0xa5	CYRILLIC SMALL LETTER LIGATURE EN GHE
					*p = 0x04A4;	//	Ҥ	0xd2 0xa4	CYRILLIC CAPITAL LETTER LIGATURE EN GHE
					break;
				case 0x04A7:		//	ҧ	0xd2 0xa7	CYRILLIC SMALL LETTER PE WITH MIDDLE HOOK
					*p = 0x04A6;	//	Ҧ	0xd2 0xa6	CYRILLIC CAPITAL LETTER PE WITH MIDDLE HOOK
					break;
				case 0x04A9:		//	ҩ	0xd2 0xa9	CYRILLIC SMALL LETTER ABKHASIAN HA
					*p = 0x04A8;	//	Ҩ	0xd2 0xa8	CYRILLIC CAPITAL LETTER ABKHASIAN HA
					break;
				case 0x04AB:		//	ҫ	0xd2 0xab	CYRILLIC SMALL LETTER ES WITH DESCENDER
					*p = 0x04AA;	//	Ҫ	0xd2 0xaa	CYRILLIC CAPITAL LETTER ES WITH DESCENDER
					break;
				case 0x04AD:		//	ҭ	0xd2 0xad	CYRILLIC SMALL LETTER TE WITH DESCENDER
					*p = 0x04AC;	//	Ҭ	0xd2 0xac	CYRILLIC CAPITAL LETTER TE WITH DESCENDER
					break;
				case 0x04AF:		//	ү	0xd2 0xaf	CYRILLIC SMALL LETTER STRAIGHT U
					*p = 0x04AE;	//	Ү	0xd2 0xae	CYRILLIC CAPITAL LETTER STRAIGHT U
					break;
				case 0x04B1:		//	ұ	0xd2 0xb1	CYRILLIC SMALL LETTER STRAIGHT U WITH STROKE
					*p = 0x04B0;	//	Ұ	0xd2 0xb0	CYRILLIC CAPITAL LETTER STRAIGHT U WITH STROKE
					break;
				case 0x04B3:		//	ҳ	0xd2 0xb3	CYRILLIC SMALL LETTER HA WITH DESCENDER
					*p = 0x04B2;	//	Ҳ	0xd2 0xb2	CYRILLIC CAPITAL LETTER HA WITH DESCENDER
					break;
				case 0x04B5:		//	ҵ	0xd2 0xb5	CYRILLIC SMALL LETTER LIGATURE TE TSE
					*p = 0x04B4;	//	Ҵ	0xd2 0xb4	CYRILLIC CAPITAL LETTER LIGATURE TE TSE
					break;
				case 0x04B7:		//	ҷ	0xd2 0xb7	CYRILLIC SMALL LETTER CHE WITH DESCENDER
					*p = 0x04B6;	//	Ҷ	0xd2 0xb6	CYRILLIC CAPITAL LETTER CHE WITH DESCENDER
					break;
				case 0x04B9:		//	ҹ	0xd2 0xb9	CYRILLIC SMALL LETTER CHE WITH VERTICAL STROKE
					*p = 0x04B8;	//	Ҹ	0xd2 0xb8	CYRILLIC CAPITAL LETTER CHE WITH VERTICAL STROKE
					break;
				case 0x04BB:		//	һ	0xd2 0xbb	CYRILLIC SMALL LETTER SHHA
					*p = 0x04BA;	//	Һ	0xd2 0xba	CYRILLIC CAPITAL LETTER SHHA
					break;
				case 0x04BD:		//	ҽ	0xd2 0xbd	CYRILLIC SMALL LETTER ABKHASIAN CHE
					*p = 0x04BC;	//	Ҽ	0xd2 0xbc	CYRILLIC CAPITAL LETTER ABKHASIAN CHE
					break;
				case 0x04BF:		//	ҿ	0xd2 0xbf	CYRILLIC SMALL LETTER ABKHASIAN CHE WITH DESCENDER
					*p = 0x04BE;	//	Ҿ	0xd2 0xbe	CYRILLIC CAPITAL LETTER ABKHASIAN CHE WITH DESCENDER
					break;
				case 0x04C2:		//	ӂ	0xd3 0x82	CYRILLIC SMALL LETTER ZHE WITH BREVE
					*p = 0x04C1;	//	Ӂ	0xd3 0x81	CYRILLIC CAPITAL LETTER ZHE WITH BREVE
					break;
				case 0x04C4:		//	ӄ	0xd3 0x84	CYRILLIC SMALL LETTER KA WITH HOOK
					*p = 0x04C3;	//	Ӄ	0xd3 0x83	CYRILLIC CAPITAL LETTER KA WITH HOOK
					break;
				case 0x04C6:		//	ӆ	0xd3 0x86	CYRILLIC SMALL LETTER EL WITH TAIL
					*p = 0x04C5;	//	Ӆ	0xd3 0x85	CYRILLIC CAPITAL LETTER EL WITH TAIL
					break;
				case 0x04C8:		//	ӈ	0xd3 0x88	CYRILLIC SMALL LETTER EN WITH HOOK
					*p = 0x04C7;	//	Ӈ	0xd3 0x87	CYRILLIC CAPITAL LETTER EN WITH HOOK
					break;
				case 0x04CA:		//	ӊ	0xd3 0x8a	CYRILLIC SMALL LETTER EN WITH TAIL
					*p = 0x04C9;	//	Ӊ	0xd3 0x89	CYRILLIC CAPITAL LETTER EN WITH TAIL
					break;
				case 0x04CC:		//	ӌ	0xd3 0x8c	CYRILLIC SMALL LETTER KHAKASSIAN CHE
					*p = 0x04CB;	//	Ӌ	0xd3 0x8b	CYRILLIC CAPITAL LETTER KHAKASSIAN CHE
					break;
				case 0x04CE:		//	ӎ	0xd3 0x8e	CYRILLIC SMALL LETTER EM WITH TAIL
					*p = 0x04CD;	//	Ӎ	0xd3 0x8d	CYRILLIC CAPITAL LETTER EM WITH TAIL
					break;
				case 0x04CF:		//	ӏ	0xd3 0x8f	CYRILLIC SMALL LETTER PALOCHKA
					*p = 0x04C0;	//	Ӏ	0xd3 0x80	CYRILLIC CAPITAL LETTER PALOCHKA
					break;
				case 0x04D1:		//	ӑ	0xd3 0x91	CYRILLIC SMALL LETTER A WITH BREVE
					*p = 0x04D0;	//	Ӑ	0xd3 0x90	CYRILLIC CAPITAL LETTER A WITH BREVE
					break;
				case 0x04D3:		//	ӓ	0xd3 0x93	CYRILLIC SMALL LETTER A WITH DIAERESIS
					*p = 0x04D2;	//	Ӓ	0xd3 0x92	CYRILLIC CAPITAL LETTER A WITH DIAERESIS
					break;
				case 0x04D5:		//	ӕ	0xd3 0x95	CYRILLIC SMALL LETTER LIGATURE A IE
					*p = 0x04D4;	//	Ӕ	0xd3 0x94	CYRILLIC CAPITAL LETTER LIGATURE A IE
					break;
				case 0x04D7:		//	ӗ	0xd3 0x97	CYRILLIC SMALL LETTER IE WITH BREVE
					*p = 0x04D6;	//	Ӗ	0xd3 0x96	CYRILLIC CAPITAL LETTER IE WITH BREVE
					break;
				case 0x04D9:		//	ә	0xd3 0x99	CYRILLIC SMALL LETTER SCHWA
					*p = 0x04D8;	//	Ә	0xd3 0x98	CYRILLIC CAPITAL LETTER SCHWA
					break;
				case 0x04DB:		//	ӛ	0xd3 0x9b	CYRILLIC SMALL LETTER SCHWA WITH DIAERESIS
					*p = 0x04DA;	//	Ӛ	0xd3 0x9a	CYRILLIC CAPITAL LETTER SCHWA WITH DIAERESIS
					break;
				case 0x04DD:		//	ӝ	0xd3 0x9d	CYRILLIC SMALL LETTER ZHE WITH DIAERESIS
					*p = 0x04DC;	//	Ӝ	0xd3 0x9c	CYRILLIC CAPITAL LETTER ZHE WITH DIAERESIS
					break;
				case 0x04DF:		//	ӟ	0xd3 0x9f	CYRILLIC SMALL LETTER ZE WITH DIAERESIS
					*p = 0x04DE;	//	Ӟ	0xd3 0x9e	CYRILLIC CAPITAL LETTER ZE WITH DIAERESIS
					break;
				case 0x04E1:		//	ӡ	0xd3 0xa1	CYRILLIC SMALL LETTER ABKHASIAN DZE
					*p = 0x04E0;	//	Ӡ	0xd3 0xa0	CYRILLIC CAPITAL LETTER ABKHASIAN DZE
					break;
				case 0x04E3:		//	ӣ	0xd3 0xa3	CYRILLIC SMALL LETTER I WITH MACRON
					*p = 0x04E2;	//	Ӣ	0xd3 0xa2	CYRILLIC CAPITAL LETTER I WITH MACRON
					break;
				case 0x04E5:		//	ӥ	0xd3 0xa5	CYRILLIC SMALL LETTER I WITH DIAERESIS
					*p = 0x04E4;	//	Ӥ	0xd3 0xa4	CYRILLIC CAPITAL LETTER I WITH DIAERESIS
					break;
				case 0x04E7:		//	ӧ	0xd3 0xa7	CYRILLIC SMALL LETTER O WITH DIAERESIS
					*p = 0x04E6;	//	Ӧ	0xd3 0xa6	CYRILLIC CAPITAL LETTER O WITH DIAERESIS
					break;
				case 0x04E9:		//	ө	0xd3 0xa9	CYRILLIC SMALL LETTER BARRED O
					*p = 0x04E8;	//	Ө	0xd3 0xa8	CYRILLIC CAPITAL LETTER BARRED O
					break;
				case 0x04EB:		//	ӫ	0xd3 0xab	CYRILLIC SMALL LETTER BARRED O WITH DIAERESIS
					*p = 0x04EA;	//	Ӫ	0xd3 0xaa	CYRILLIC CAPITAL LETTER BARRED O WITH DIAERESIS
					break;
				case 0x04ED:		//	ӭ	0xd3 0xad	CYRILLIC SMALL LETTER E WITH DIAERESIS
					*p = 0x04EC;	//	Ӭ	0xd3 0xac	CYRILLIC CAPITAL LETTER E WITH DIAERESIS
					break;
				case 0x04EF:		//	ӯ	0xd3 0xaf	CYRILLIC SMALL LETTER U WITH MACRON
					*p = 0x04EE;	//	Ӯ	0xd3 0xae	CYRILLIC CAPITAL LETTER U WITH MACRON
					break;
				case 0x04F1:		//	ӱ	0xd3 0xb1	CYRILLIC SMALL LETTER U WITH DIAERESIS
					*p = 0x04F0;	//	Ӱ	0xd3 0xb0	CYRILLIC CAPITAL LETTER U WITH DIAERESIS
					break;
				case 0x04F3:		//	ӳ	0xd3 0xb3	CYRILLIC SMALL LETTER U WITH DOUBLE ACUTE
					*p = 0x04F2;	//	Ӳ	0xd3 0xb2	CYRILLIC CAPITAL LETTER U WITH DOUBLE ACUTE
					break;
				case 0x04F5:		//	ӵ	0xd3 0xb5	CYRILLIC SMALL LETTER CHE WITH DIAERESIS
					*p = 0x04F4;	//	Ӵ	0xd3 0xb4	CYRILLIC CAPITAL LETTER CHE WITH DIAERESIS
					break;
				case 0x04F7:		//	ӷ	0xd3 0xb7	CYRILLIC SMALL LETTER GHE WITH DESCENDER
					*p = 0x04F6;	//	Ӷ	0xd3 0xb6	CYRILLIC CAPITAL LETTER GHE WITH DESCENDER
					break;
				case 0x04F9:		//	ӹ	0xd3 0xb9	CYRILLIC SMALL LETTER YERU WITH DIAERESIS
					*p = 0x04F8;	//	Ӹ	0xd3 0xb8	CYRILLIC CAPITAL LETTER YERU WITH DIAERESIS
					break;
				case 0x04FB:		//	ӻ	0xd3 0xbb	CYRILLIC SMALL LETTER GHE WITH STROKE AND HOOK
					*p = 0x04FA;	//	Ӻ	0xd3 0xba	CYRILLIC CAPITAL LETTER GHE WITH STROKE AND HOOK
					break;
				case 0x04FD:		//	ӽ	0xd3 0xbd	CYRILLIC SMALL LETTER HA WITH HOOK
					*p = 0x04FC;	//	Ӽ	0xd3 0xbc	CYRILLIC CAPITAL LETTER HA WITH HOOK
					break;
				case 0x04FF:		//	ӿ	0xd3 0xbf	CYRILLIC SMALL LETTER HA WITH STROKE
					*p = 0x04FE;	//	Ӿ	0xd3 0xbe	CYRILLIC CAPITAL LETTER HA WITH STROKE
					break;
				case 0x0501:		//	ԁ	0xd4 0x81	CYRILLIC SMALL LETTER KOMI DE
					*p = 0x0500;	//	Ԁ	0xd4 0x80	CYRILLIC CAPITAL LETTER KOMI DE
					break;
				case 0x0503:		//	ԃ	0xd4 0x83	CYRILLIC SMALL LETTER KOMI DJE
					*p = 0x0502;	//	Ԃ	0xd4 0x82	CYRILLIC CAPITAL LETTER KOMI DJE
					break;
				case 0x0505:		//	ԅ	0xd4 0x85	CYRILLIC SMALL LETTER KOMI ZJE
					*p = 0x0504;	//	Ԅ	0xd4 0x84	CYRILLIC CAPITAL LETTER KOMI ZJE
					break;
				case 0x0507:		//	ԇ	0xd4 0x87	CYRILLIC SMALL LETTER KOMI DZJE
					*p = 0x0506;	//	Ԇ	0xd4 0x86	CYRILLIC CAPITAL LETTER KOMI DZJE
					break;
				case 0x0509:		//	ԉ	0xd4 0x89	CYRILLIC SMALL LETTER KOMI LJE
					*p = 0x0508;	//	Ԉ	0xd4 0x88	CYRILLIC CAPITAL LETTER KOMI LJE
					break;
				case 0x050B:		//	ԋ	0xd4 0x8b	CYRILLIC SMALL LETTER KOMI NJE
					*p = 0x050A;	//	Ԋ	0xd4 0x8a	CYRILLIC CAPITAL LETTER KOMI NJE
					break;
				case 0x050D:		//	ԍ	0xd4 0x8d	CYRILLIC SMALL LETTER KOMI SJE
					*p = 0x050C;	//	Ԍ	0xd4 0x8c	CYRILLIC CAPITAL LETTER KOMI SJE
					break;
				case 0x050F:		//	ԏ	0xd4 0x8f	CYRILLIC SMALL LETTER KOMI TJE
					*p = 0x050E;	//	Ԏ	0xd4 0x8e	CYRILLIC CAPITAL LETTER KOMI TJE
					break;
				case 0x0511:		//	ԑ	0xd4 0x91	CYRILLIC SMALL LETTER REVERSED ZE
					*p = 0x0510;	//	Ԑ	0xd4 0x90	CYRILLIC CAPITAL LETTER REVERSED ZE
					break;
				case 0x0513:		//	ԓ	0xd4 0x93	CYRILLIC SMALL LETTER EL WITH HOOK
					*p = 0x0512;	//	Ԓ	0xd4 0x92	CYRILLIC CAPITAL LETTER EL WITH HOOK
					break;
				case 0x0515:		//	ԕ	0xd4 0x95	CYRILLIC SMALL LETTER LHA
					*p = 0x0514;	//	Ԕ	0xd4 0x94	CYRILLIC CAPITAL LETTER LHA
					break;
				case 0x0517:		//	ԗ	0xd4 0x97	CYRILLIC SMALL LETTER RHA
					*p = 0x0516;	//	Ԗ	0xd4 0x96	CYRILLIC CAPITAL LETTER RHA
					break;
				case 0x0519:		//	ԙ	0xd4 0x99	CYRILLIC SMALL LETTER YAE
					*p = 0x0518;	//	Ԙ	0xd4 0x98	CYRILLIC CAPITAL LETTER YAE
					break;
				case 0x051B:		//	ԛ	0xd4 0x9b	CYRILLIC SMALL LETTER QA
					*p = 0x051A;	//	Ԛ	0xd4 0x9a	CYRILLIC CAPITAL LETTER QA
					break;
				case 0x051D:		//	ԝ	0xd4 0x9d	CYRILLIC SMALL LETTER WE
					*p = 0x051C;	//	Ԝ	0xd4 0x9c	CYRILLIC CAPITAL LETTER WE
					break;
				case 0x051F:		//	ԟ	0xd4 0x9f	CYRILLIC SMALL LETTER ALEUT KA
					*p = 0x051E;	//	Ԟ	0xd4 0x9e	CYRILLIC CAPITAL LETTER ALEUT KA
					break;
				case 0x0521:		//	ԡ	0xd4 0xa1	CYRILLIC SMALL LETTER EL WITH MIDDLE HOOK
					*p = 0x0520;	//	Ԡ	0xd4 0xa0	CYRILLIC CAPITAL LETTER EL WITH MIDDLE HOOK
					break;
				case 0x0523:		//	ԣ	0xd4 0xa3	CYRILLIC SMALL LETTER EN WITH MIDDLE HOOK
					*p = 0x0522;	//	Ԣ	0xd4 0xa2	CYRILLIC CAPITAL LETTER EN WITH MIDDLE HOOK
					break;
				case 0x0525:		//	ԥ	0xd4 0xa5	CYRILLIC SMALL LETTER PE WITH DESCENDER
					*p = 0x0524;	//	Ԥ	0xd4 0xa4	CYRILLIC CAPITAL LETTER PE WITH DESCENDER
					break;
				case 0x0527:		//	ԧ	0xd4 0xa7	CYRILLIC SMALL LETTER SHHA WITH DESCENDER
					*p = 0x0526;	//	Ԧ	0xd4 0xa6	CYRILLIC CAPITAL LETTER SHHA WITH DESCENDER
					break;
				case 0x0529:		//	ԩ	0xd4 0xa9	CYRILLIC SMALL LETTER EN WITH LEFT HOOK
					*p = 0x0528;	//	Ԩ	0xd4 0xa8	CYRILLIC CAPITAL LETTER EN WITH LEFT HOOK
					break;
				case 0x052B:		//	ԫ	0xd4 0xab	CYRILLIC SMALL LETTER DZZHE
					*p = 0x052A;	//	Ԫ	0xd4 0xaa	CYRILLIC CAPITAL LETTER DZZHE
					break;
				case 0x052D:		//	ԭ	0xd4 0xad	CYRILLIC SMALL LETTER DCHE
					*p = 0x052C;	//	Ԭ	0xd4 0xac	CYRILLIC CAPITAL LETTER DCHE
					break;
				case 0x052F:		//	ԯ	0xd4 0xaf	CYRILLIC SMALL LETTER EL WITH DESCENDER
					*p = 0x052E;	//	Ԯ	0xd4 0xae	CYRILLIC CAPITAL LETTER EL WITH DESCENDER
					break;
				case 0x0561:		//	ա	0xd5 0xa1	ARMENIAN SMALL LETTER AYB
					*p = 0x0531;	//	Ա	0xd4 0xb1	ARMENIAN CAPITAL LETTER AYB
					break;
				case 0x0562:		//	բ	0xd5 0xa2	ARMENIAN SMALL LETTER BEN
					*p = 0x0532;	//	Բ	0xd4 0xb2	ARMENIAN CAPITAL LETTER BEN
					break;
				case 0x0563:		//	գ	0xd5 0xa3	ARMENIAN SMALL LETTER GIM
					*p = 0x0533;	//	Գ	0xd4 0xb3	ARMENIAN CAPITAL LETTER GIM
					break;
				case 0x0564:		//	դ	0xd5 0xa4	ARMENIAN SMALL LETTER DA
					*p = 0x0534;	//	Դ	0xd4 0xb4	ARMENIAN CAPITAL LETTER DA
					break;
				case 0x0565:		//	ե	0xd5 0xa5	ARMENIAN SMALL LETTER ECH
					*p = 0x0535;	//	Ե	0xd4 0xb5	ARMENIAN CAPITAL LETTER ECH
					break;
				case 0x0566:		//	զ	0xd5 0xa6	ARMENIAN SMALL LETTER ZA
					*p = 0x0536;	//	Զ	0xd4 0xb6	ARMENIAN CAPITAL LETTER ZA
					break;
				case 0x0567:		//	է	0xd5 0xa7	ARMENIAN SMALL LETTER EH
					*p = 0x0537;	//	Է	0xd4 0xb7	ARMENIAN CAPITAL LETTER EH
					break;
				case 0x0568:		//	ը	0xd5 0xa8	ARMENIAN SMALL LETTER ET
					*p = 0x0538;	//	Ը	0xd4 0xb8	ARMENIAN CAPITAL LETTER ET
					break;
				case 0x0569:		//	թ	0xd5 0xa9	ARMENIAN SMALL LETTER TO
					*p = 0x0539;	//	Թ	0xd4 0xb9	ARMENIAN CAPITAL LETTER TO
					break;
				case 0x056A:		//	ժ	0xd5 0xaa	ARMENIAN SMALL LETTER ZHE
					*p = 0x053A;	//	Ժ	0xd4 0xba	ARMENIAN CAPITAL LETTER ZHE
					break;
				case 0x056B:		//	ի	0xd5 0xab	ARMENIAN SMALL LETTER INI
					*p = 0x053B;	//	Ի	0xd4 0xbb	ARMENIAN CAPITAL LETTER INI
					break;
				case 0x056C:		//	լ	0xd5 0xac	ARMENIAN SMALL LETTER LIWN
					*p = 0x053C;	//	Լ	0xd4 0xbc	ARMENIAN CAPITAL LETTER LIWN
					break;
				case 0x056D:		//	խ	0xd5 0xad	ARMENIAN SMALL LETTER XEH
					*p = 0x053D;	//	Խ	0xd4 0xbd	ARMENIAN CAPITAL LETTER XEH
					break;
				case 0x056E:		//	ծ	0xd5 0xae	ARMENIAN SMALL LETTER CA
					*p = 0x053E;	//	Ծ	0xd4 0xbe	ARMENIAN CAPITAL LETTER CA
					break;
				case 0x056F:		//	կ	0xd5 0xaf	ARMENIAN SMALL LETTER KEN
					*p = 0x053F;	//	Կ	0xd4 0xbf	ARMENIAN CAPITAL LETTER KEN
					break;
				case 0x0570:		//	հ	0xd5 0xb0	ARMENIAN SMALL LETTER HO
					*p = 0x0540;	//	Հ	0xd5 0x80	ARMENIAN CAPITAL LETTER HO
					break;
				case 0x0571:		//	ձ	0xd5 0xb1	ARMENIAN SMALL LETTER JA
					*p = 0x0541;	//	Ձ	0xd5 0x81	ARMENIAN CAPITAL LETTER JA
					break;
				case 0x0572:		//	ղ	0xd5 0xb2	ARMENIAN SMALL LETTER GHAD
					*p = 0x0542;	//	Ղ	0xd5 0x82	ARMENIAN CAPITAL LETTER GHAD
					break;
				case 0x0573:		//	ճ	0xd5 0xb3	ARMENIAN SMALL LETTER CHEH
					*p = 0x0543;	//	Ճ	0xd5 0x83	ARMENIAN CAPITAL LETTER CHEH
					break;
				case 0x0574:		//	մ	0xd5 0xb4	ARMENIAN SMALL LETTER MEN
					*p = 0x0544;	//	Մ	0xd5 0x84	ARMENIAN CAPITAL LETTER MEN
					break;
				case 0x0575:		//	յ	0xd5 0xb5	ARMENIAN SMALL LETTER YI
					*p = 0x0545;	//	Յ	0xd5 0x85	ARMENIAN CAPITAL LETTER YI
					break;
				case 0x0576:		//	ն	0xd5 0xb6	ARMENIAN SMALL LETTER NOW
					*p = 0x0546;	//	Ն	0xd5 0x86	ARMENIAN CAPITAL LETTER NOW
					break;
				case 0x0577:		//	շ	0xd5 0xb7	ARMENIAN SMALL LETTER SHA
					*p = 0x0547;	//	Շ	0xd5 0x87	ARMENIAN CAPITAL LETTER SHA
					break;
				case 0x0578:		//	ո	0xd5 0xb8	ARMENIAN SMALL LETTER VO
					*p = 0x0548;	//	Ո	0xd5 0x88	ARMENIAN CAPITAL LETTER VO
					break;
				case 0x0579:		//	չ	0xd5 0xb9	ARMENIAN SMALL LETTER CHA
					*p = 0x0549;	//	Չ	0xd5 0x89	ARMENIAN CAPITAL LETTER CHA
					break;
				case 0x057A:		//	պ	0xd5 0xba	ARMENIAN SMALL LETTER PEH
					*p = 0x054A;	//	Պ	0xd5 0x8a	ARMENIAN CAPITAL LETTER PEH
					break;
				case 0x057B:		//	ջ	0xd5 0xbb	ARMENIAN SMALL LETTER JHEH
					*p = 0x054B;	//	Ջ	0xd5 0x8b	ARMENIAN CAPITAL LETTER JHEH
					break;
				case 0x057C:		//	ռ	0xd5 0xbc	ARMENIAN SMALL LETTER RA
					*p = 0x054C;	//	Ռ	0xd5 0x8c	ARMENIAN CAPITAL LETTER RA
					break;
				case 0x057D:		//	ս	0xd5 0xbd	ARMENIAN SMALL LETTER SEH
					*p = 0x054D;	//	Ս	0xd5 0x8d	ARMENIAN CAPITAL LETTER SEH
					break;
				case 0x057E:		//	վ	0xd5 0xbe	ARMENIAN SMALL LETTER VEW
					*p = 0x054E;	//	Վ	0xd5 0x8e	ARMENIAN CAPITAL LETTER VEW
					break;
				case 0x057F:		//	տ	0xd5 0xbf	ARMENIAN SMALL LETTER TIWN
					*p = 0x054F;	//	Տ	0xd5 0x8f	ARMENIAN CAPITAL LETTER TIWN
					break;
				case 0x0580:		//	ր	0xd6 0x80	ARMENIAN SMALL LETTER REH
					*p = 0x0550;	//	Ր	0xd5 0x90	ARMENIAN CAPITAL LETTER REH
					break;
				case 0x0581:		//	ց	0xd6 0x81	ARMENIAN SMALL LETTER CO
					*p = 0x0551;	//	Ց	0xd5 0x91	ARMENIAN CAPITAL LETTER CO
					break;
				case 0x0582:		//	ւ	0xd6 0x82	ARMENIAN SMALL LETTER YIWN
					*p = 0x0552;	//	Ւ	0xd5 0x92	ARMENIAN CAPITAL LETTER YIWN
					break;
				case 0x0583:		//	փ	0xd6 0x83	ARMENIAN SMALL LETTER PIWR
					*p = 0x0553;	//	Փ	0xd5 0x93	ARMENIAN CAPITAL LETTER PIWR
					break;
				case 0x0584:		//	ք	0xd6 0x84	ARMENIAN SMALL LETTER KEH
					*p = 0x0554;	//	Ք	0xd5 0x94	ARMENIAN CAPITAL LETTER KEH
					break;
				case 0x0585:		//	օ	0xd6 0x85	ARMENIAN SMALL LETTER OH
					*p = 0x0555;	//	Օ	0xd5 0x95	ARMENIAN CAPITAL LETTER OH
					break;
				case 0x0586:		//	ֆ	0xd6 0x86	ARMENIAN SMALL LETTER FEH
					*p = 0x0556;	//	Ֆ	0xd5 0x96	ARMENIAN CAPITAL LETTER FEH
					break;
				case 0x10A0:		//	Ⴀ	0xe1 0x82 0xa0	GEORGIAN LETTER AN
					*p = 0x1C90;	//	Ა	0xe1 0xb2 0x90	GEORGIAN CAPITAL LETTER AN
					break;
				case 0x10A1:		//	Ⴁ	0xe1 0x82 0xa1	GEORGIAN LETTER BAN
					*p = 0x1C91;	//	Ბ	0xe1 0xb2 0x91	GEORGIAN CAPITAL LETTER BAN
					break;
				case 0x10A2:		//	Ⴂ	0xe1 0x82 0xa2	GEORGIAN LETTER GAN
					*p = 0x1C92;	//	Გ	0xe1 0xb2 0x92	GEORGIAN CAPITAL LETTER GAN
					break;
				case 0x10A3:		//	Ⴃ	0xe1 0x82 0xa3	GEORGIAN LETTER DON
					*p = 0x1C93;	//	Დ	0xe1 0xb2 0x93	GEORGIAN CAPITAL LETTER DON
					break;
				case 0x10A4:		//	Ⴄ	0xe1 0x82 0xa4	GEORGIAN LETTER EN
					*p = 0x1C94;	//	Ე	0xe1 0xb2 0x94	GEORGIAN CAPITAL LETTER EN
					break;
				case 0x10A5:		//	Ⴅ	0xe1 0x82 0xa5	GEORGIAN LETTER VIN
					*p = 0x1C95;	//	Ვ	0xe1 0xb2 0x95	GEORGIAN CAPITAL LETTER VIN
					break;
				case 0x10A6:		//	Ⴆ	0xe1 0x82 0xa6	GEORGIAN LETTER ZEN
					*p = 0x1C96;	//	Ზ	0xe1 0xb2 0x96	GEORGIAN CAPITAL LETTER ZEN
					break;
				case 0x10A7:		//	Ⴇ	0xe1 0x82 0xa7	GEORGIAN LETTER TAN
					*p = 0x1C97;	//	Თ	0xe1 0xb2 0x97	GEORGIAN CAPITAL LETTER TAN
					break;
				case 0x10A8:		//	Ⴈ	0xe1 0x82 0xa8	GEORGIAN LETTER IN
					*p = 0x1C98;	//	Ი	0xe1 0xb2 0x98	GEORGIAN CAPITAL LETTER IN
					break;
				case 0x10A9:		//	Ⴉ	0xe1 0x82 0xa9	GEORGIAN LETTER KAN
					*p = 0x1C99;	//	Კ	0xe1 0xb2 0x99	GEORGIAN CAPITAL LETTER KAN
					break;
				case 0x10AA:		//	Ⴊ	0xe1 0x82 0xaa	GEORGIAN LETTER LAS
					*p = 0x1C9A;	//	Ლ	0xe1 0xb2 0x9a	GEORGIAN CAPITAL LETTER LAS
					break;
				case 0x10AB:		//	Ⴋ	0xe1 0x82 0xab	GEORGIAN LETTER MAN
					*p = 0x1C9B;	//	Მ	0xe1 0xb2 0x9b	GEORGIAN CAPITAL LETTER MAN
					break;
				case 0x10AC:		//	Ⴌ	0xe1 0x82 0xac	GEORGIAN LETTER NAR
					*p = 0x1C9C;	//	Ნ	0xe1 0xb2 0x9c	GEORGIAN CAPITAL LETTER NAR
					break;
				case 0x10AD:		//	Ⴍ	0xe1 0x82 0xad	GEORGIAN LETTER ON
					*p = 0x1C9D;	//	Ო	0xe1 0xb2 0x9d	GEORGIAN CAPITAL LETTER ON
					break;
				case 0x10AE:		//	Ⴎ	0xe1 0x82 0xae	GEORGIAN LETTER PAR
					*p = 0x1C9E;	//	Პ	0xe1 0xb2 0x9e	GEORGIAN CAPITAL LETTER PAR
					break;
				case 0x10AF:		//	Ⴏ	0xe1 0x82 0xaf	GEORGIAN LETTER ZHAR
					*p = 0x1C9F;	//	Ჟ	0xe1 0xb2 0x9f	GEORGIAN CAPITAL LETTER ZHAR
					break;
				case 0x10B0:		//	Ⴐ	0xe1 0x82 0xb0	GEORGIAN LETTER RAE
					*p = 0x1CA0;	//	Რ	0xe1 0xb2 0xa0	GEORGIAN CAPITAL LETTER RAE
					break;
				case 0x10B1:		//	Ⴑ	0xe1 0x82 0xb1	GEORGIAN LETTER SAN
					*p = 0x1CA1;	//	Ს	0xe1 0xb2 0xa1	GEORGIAN CAPITAL LETTER SAN
					break;
				case 0x10B2:		//	Ⴒ	0xe1 0x82 0xb2	GEORGIAN LETTER TAR
					*p = 0x1CA2;	//	Ტ	0xe1 0xb2 0xa2	GEORGIAN CAPITAL LETTER TAR
					break;
				case 0x10B3:		//	Ⴓ	0xe1 0x82 0xb3	GEORGIAN LETTER UN
					*p = 0x1CA3;	//	Უ	0xe1 0xb2 0xa3	GEORGIAN CAPITAL LETTER UN
					break;
				case 0x10B4:		//	Ⴔ	0xe1 0x82 0xb4	GEORGIAN LETTER PHAR
					*p = 0x1CA4;	//	Ფ	0xe1 0xb2 0xa4	GEORGIAN CAPITAL LETTER PHAR
					break;
				case 0x10B5:		//	Ⴕ	0xe1 0x82 0xb5	GEORGIAN LETTER KHAR
					*p = 0x1CA5;	//	Ქ	0xe1 0xb2 0xa5	GEORGIAN CAPITAL LETTER KHAR
					break;
				case 0x10B6:		//	Ⴖ	0xe1 0x82 0xb6	GEORGIAN LETTER GHAN
					*p = 0x1CA6;	//	Ღ	0xe1 0xb2 0xa6	GEORGIAN CAPITAL LETTER GHAN
					break;
				case 0x10B7:		//	Ⴗ	0xe1 0x82 0xb7	GEORGIAN LETTER QAR
					*p = 0x1CA7;	//	Ყ	0xe1 0xb2 0xa7	GEORGIAN CAPITAL LETTER QAR
					break;
				case 0x10B8:		//	Ⴘ	0xe1 0x82 0xb8	GEORGIAN LETTER SHIN
					*p = 0x1CA8;	//	Შ	0xe1 0xb2 0xa8	GEORGIAN CAPITAL LETTER SHIN
					break;
				case 0x10B9:		//	Ⴙ	0xe1 0x82 0xb9	GEORGIAN LETTER CHIN
					*p = 0x1CA9;	//	Ჩ	0xe1 0xb2 0xa9	GEORGIAN CAPITAL LETTER CHIN
					break;
				case 0x10BA:		//	Ⴚ	0xe1 0x82 0xba	GEORGIAN LETTER CAN
					*p = 0x1CAA;	//	Ც	0xe1 0xb2 0xaa	GEORGIAN CAPITAL LETTER CAN
					break;
				case 0x10BB:		//	Ⴛ	0xe1 0x82 0xbb	GEORGIAN LETTER JIL
					*p = 0x1CAB;	//	Ძ	0xe1 0xb2 0xab	GEORGIAN CAPITAL LETTER JIL
					break;
				case 0x10BC:		//	Ⴜ	0xe1 0x82 0xbc	GEORGIAN LETTER CIL
					*p = 0x1CAC;	//	Წ	0xe1 0xb2 0xac	GEORGIAN CAPITAL LETTER CIL
					break;
				case 0x10BD:		//	Ⴝ	0xe1 0x82 0xbd	GEORGIAN LETTER CHAR
					*p = 0x1CAD;	//	Ჭ	0xe1 0xb2 0xad	GEORGIAN CAPITAL LETTER CHAR
					break;
				case 0x10BE:		//	Ⴞ	0xe1 0x82 0xbe	GEORGIAN LETTER XAN
					*p = 0x1CAE;	//	Ხ	0xe1 0xb2 0xae	GEORGIAN CAPITAL LETTER XAN
					break;
				case 0x10BF:		//	Ⴟ	0xe1 0x82 0xbf	GEORGIAN LETTER JHAN
					*p = 0x1CAF;	//	Ჯ	0xe1 0xb2 0xaf	GEORGIAN CAPITAL LETTER JHAN
					break;
				case 0x10C0:		//	Ⴠ	0xe1 0x83 0x80	GEORGIAN LETTER HAE
					*p = 0x1CB0;	//	Ჰ	0xe1 0xb2 0xb0	GEORGIAN CAPITAL LETTER HAE
					break;
				case 0x10C1:		//	Ⴡ	0xe1 0x83 0x81	GEORGIAN LETTER HE
					*p = 0x1CB1;	//	Ჱ	0xe1 0xb2 0xb1	GEORGIAN CAPITAL LETTER HE
					break;
				case 0x10C2:		//	Ⴢ	0xe1 0x83 0x82	GEORGIAN LETTER HIE
					*p = 0x1CB2;	//	Ჲ	0xe1 0xb2 0xb2	GEORGIAN CAPITAL LETTER HIE
					break;
				case 0x10C3:		//	Ⴣ	0xe1 0x83 0x83	GEORGIAN LETTER WE
					*p = 0x1CB3;	//	Ჳ	0xe1 0xb2 0xb3	GEORGIAN CAPITAL LETTER WE
					break;
				case 0x10C4:		//	Ⴤ	0xe1 0x83 0x84	GEORGIAN LETTER HAR
					*p = 0x1CB4;	//	Ჴ	0xe1 0xb2 0xb4	GEORGIAN CAPITAL LETTER HAR
					break;
				case 0x10C5:		//	Ⴥ	0xe1 0x83 0x85	GEORGIAN LETTER HOE
					*p = 0x1CB5;	//	Ჵ	0xe1 0xb2 0xb5	GEORGIAN CAPITAL LETTER HOE
					break;
				case 0x10C7:		//	Ⴧ	0xe1 0x83 0x87	GEORGIAN LETTER YN
					*p = 0x1CB7;	//	Ჷ	0xe1 0xb2 0xb7	GEORGIAN CAPITAL LETTER YN
					break;
				case 0x10CD:		//	Ⴭ	0xe1 0x83 0x8d	GEORGIAN LETTER AEN
					*p = 0x1CBD;	//	Ჽ	0xe1 0xb2 0xbd	GEORGIAN CAPITAL LETTER AEN
					break;
				case 0x10D0:		//	ა	0xe1 0x83 0x90	GEORGIAN SMALL LETTER AN
					*p = 0x1C90;	//	Ა	0xe1 0xb2 0x90	GEORGIAN CAPITAL LETTER AN
					break;
				case 0x10D1:		//	ბ	0xe1 0x83 0x91	GEORGIAN SMALL LETTER BAN
					*p = 0x1C91;	//	Ბ	0xe1 0xb2 0x91	GEORGIAN CAPITAL LETTER BAN
					break;
				case 0x10D2:		//	გ	0xe1 0x83 0x92	GEORGIAN SMALL LETTER GAN
					*p = 0x1C92;	//	Გ	0xe1 0xb2 0x92	GEORGIAN CAPITAL LETTER GAN
					break;
				case 0x10D3:		//	დ	0xe1 0x83 0x93	GEORGIAN SMALL LETTER DON
					*p = 0x1C93;	//	Დ	0xe1 0xb2 0x93	GEORGIAN CAPITAL LETTER DON
					break;
				case 0x10D4:		//	ე	0xe1 0x83 0x94	GEORGIAN SMALL LETTER EN
					*p = 0x1C94;	//	Ე	0xe1 0xb2 0x94	GEORGIAN CAPITAL LETTER EN
					break;
				case 0x10D5:		//	ვ	0xe1 0x83 0x95	GEORGIAN SMALL LETTER VIN
					*p = 0x1C95;	//	Ვ	0xe1 0xb2 0x95	GEORGIAN CAPITAL LETTER VIN
					break;
				case 0x10D6:		//	ზ	0xe1 0x83 0x96	GEORGIAN SMALL LETTER ZEN
					*p = 0x1C96;	//	Ზ	0xe1 0xb2 0x96	GEORGIAN CAPITAL LETTER ZEN
					break;
				case 0x10D7:		//	თ	0xe1 0x83 0x97	GEORGIAN SMALL LETTER TAN
					*p = 0x1C97;	//	Თ	0xe1 0xb2 0x97	GEORGIAN CAPITAL LETTER TAN
					break;
				case 0x10D8:		//	ი	0xe1 0x83 0x98	GEORGIAN SMALL LETTER IN
					*p = 0x1C98;	//	Ი	0xe1 0xb2 0x98	GEORGIAN CAPITAL LETTER IN
					break;
				case 0x10D9:		//	კ	0xe1 0x83 0x99	GEORGIAN SMALL LETTER KAN
					*p = 0x1C99;	//	Კ	0xe1 0xb2 0x99	GEORGIAN CAPITAL LETTER KAN
					break;
				case 0x10DA:		//	ლ	0xe1 0x83 0x9a	GEORGIAN SMALL LETTER LAS
					*p = 0x1C9A;	//	Ლ	0xe1 0xb2 0x9a	GEORGIAN CAPITAL LETTER LAS
					break;
				case 0x10DB:		//	მ	0xe1 0x83 0x9b	GEORGIAN SMALL LETTER MAN
					*p = 0x1C9B;	//	Მ	0xe1 0xb2 0x9b	GEORGIAN CAPITAL LETTER MAN
					break;
				case 0x10DC:		//	ნ	0xe1 0x83 0x9c	GEORGIAN SMALL LETTER NAR
					*p = 0x1C9C;	//	Ნ	0xe1 0xb2 0x9c	GEORGIAN CAPITAL LETTER NAR
					break;
				case 0x10DD:		//	ო	0xe1 0x83 0x9d	GEORGIAN SMALL LETTER ON
					*p = 0x1C9D;	//	Ო	0xe1 0xb2 0x9d	GEORGIAN CAPITAL LETTER ON
					break;
				case 0x10DE:		//	პ	0xe1 0x83 0x9e	GEORGIAN SMALL LETTER PAR
					*p = 0x1C9E;	//	Პ	0xe1 0xb2 0x9e	GEORGIAN CAPITAL LETTER PAR
					break;
				case 0x10DF:		//	ჟ	0xe1 0x83 0x9f	GEORGIAN SMALL LETTER ZHAR
					*p = 0x1C9F;	//	Ჟ	0xe1 0xb2 0x9f	GEORGIAN CAPITAL LETTER ZHAR
					break;
				case 0x10E0:		//	რ	0xe1 0x83 0xa0	GEORGIAN SMALL LETTER RAE
					*p = 0x1CA0;	//	Რ	0xe1 0xb2 0xa0	GEORGIAN CAPITAL LETTER RAE
					break;
				case 0x10E1:		//	ს	0xe1 0x83 0xa1	GEORGIAN SMALL LETTER SAN
					*p = 0x1CA1;	//	Ს	0xe1 0xb2 0xa1	GEORGIAN CAPITAL LETTER SAN
					break;
				case 0x10E2:		//	ტ	0xe1 0x83 0xa2	GEORGIAN SMALL LETTER TAR
					*p = 0x1CA2;	//	Ტ	0xe1 0xb2 0xa2	GEORGIAN CAPITAL LETTER TAR
					break;
				case 0x10E3:		//	უ	0xe1 0x83 0xa3	GEORGIAN SMALL LETTER UN
					*p = 0x1CA3;	//	Უ	0xe1 0xb2 0xa3	GEORGIAN CAPITAL LETTER UN
					break;
				case 0x10E4:		//	ფ	0xe1 0x83 0xa4	GEORGIAN SMALL LETTER PHAR
					*p = 0x1CA4;	//	Ფ	0xe1 0xb2 0xa4	GEORGIAN CAPITAL LETTER PHAR
					break;
				case 0x10E5:		//	ქ	0xe1 0x83 0xa5	GEORGIAN SMALL LETTER KHAR
					*p = 0x1CA5;	//	Ქ	0xe1 0xb2 0xa5	GEORGIAN CAPITAL LETTER KHAR
					break;
				case 0x10E6:		//	ღ	0xe1 0x83 0xa6	GEORGIAN SMALL LETTER GHAN
					*p = 0x1CA6;	//	Ღ	0xe1 0xb2 0xa6	GEORGIAN CAPITAL LETTER GHAN
					break;
				case 0x10E7:		//	ყ	0xe1 0x83 0xa7	GEORGIAN SMALL LETTER QAR
					*p = 0x1CA7;	//	Ყ	0xe1 0xb2 0xa7	GEORGIAN CAPITAL LETTER QAR
					break;
				case 0x10E8:		//	შ	0xe1 0x83 0xa8	GEORGIAN SMALL LETTER SHIN
					*p = 0x1CA8;	//	Შ	0xe1 0xb2 0xa8	GEORGIAN CAPITAL LETTER SHIN
					break;
				case 0x10E9:		//	ჩ	0xe1 0x83 0xa9	GEORGIAN SMALL LETTER CHIN
					*p = 0x1CA9;	//	Ჩ	0xe1 0xb2 0xa9	GEORGIAN CAPITAL LETTER CHIN
					break;
				case 0x10EA:		//	ც	0xe1 0x83 0xaa	GEORGIAN SMALL LETTER CAN
					*p = 0x1CAA;	//	Ც	0xe1 0xb2 0xaa	GEORGIAN CAPITAL LETTER CAN
					break;
				case 0x10EB:		//	ძ	0xe1 0x83 0xab	GEORGIAN SMALL LETTER JIL
					*p = 0x1CAB;	//	Ძ	0xe1 0xb2 0xab	GEORGIAN CAPITAL LETTER JIL
					break;
				case 0x10EC:		//	წ	0xe1 0x83 0xac	GEORGIAN SMALL LETTER CIL
					*p = 0x1CAC;	//	Წ	0xe1 0xb2 0xac	GEORGIAN CAPITAL LETTER CIL
					break;
				case 0x10ED:		//	ჭ	0xe1 0x83 0xad	GEORGIAN SMALL LETTER CHAR
					*p = 0x1CAD;	//	Ჭ	0xe1 0xb2 0xad	GEORGIAN CAPITAL LETTER CHAR
					break;
				case 0x10EE:		//	ხ	0xe1 0x83 0xae	GEORGIAN SMALL LETTER XAN
					*p = 0x1CAE;	//	Ხ	0xe1 0xb2 0xae	GEORGIAN CAPITAL LETTER XAN
					break;
				case 0x10EF:		//	ჯ	0xe1 0x83 0xaf	GEORGIAN SMALL LETTER JHAN
					*p = 0x1CAF;	//	Ჯ	0xe1 0xb2 0xaf	GEORGIAN CAPITAL LETTER JHAN
					break;
				case 0x10F0:		//	ჰ	0xe1 0x83 0xb0	GEORGIAN SMALL LETTER HAE
					*p = 0x1CB0;	//	Ჰ	0xe1 0xb2 0xb0	GEORGIAN CAPITAL LETTER HAE
					break;
				case 0x10F1:		//	ჱ	0xe1 0x83 0xb1	GEORGIAN SMALL LETTER HE
					*p = 0x1CB1;	//	Ჱ	0xe1 0xb2 0xb1	GEORGIAN CAPITAL LETTER HE
					break;
				case 0x10F2:		//	ჲ	0xe1 0x83 0xb2	GEORGIAN SMALL LETTER HIE
					*p = 0x1CB2;	//	Ჲ	0xe1 0xb2 0xb2	GEORGIAN CAPITAL LETTER HIE
					break;
				case 0x10F3:		//	ჳ	0xe1 0x83 0xb3	GEORGIAN SMALL LETTER WE
					*p = 0x1CB3;	//	Ჳ	0xe1 0xb2 0xb3	GEORGIAN CAPITAL LETTER WE
					break;
				case 0x10F4:		//	ჴ	0xe1 0x83 0xb4	GEORGIAN SMALL LETTER HAR
					*p = 0x1CB4;	//	Ჴ	0xe1 0xb2 0xb4	GEORGIAN CAPITAL LETTER HAR
					break;
				case 0x10F5:		//	ჵ	0xe1 0x83 0xb5	GEORGIAN SMALL LETTER HOE
					*p = 0x1CB5;	//	Ჵ	0xe1 0xb2 0xb5	GEORGIAN CAPITAL LETTER HOE
					break;
				case 0x10F6:		//	ჶ	0xe1 0x83 0xb6	GEORGIAN SMALL LETTER FI
					*p = 0x1CB6;	//	Ჶ	0xe1 0xb2 0xb6	GEORGIAN CAPITAL LETTER FI
					break;
				case 0x10F7:		//	ჷ	0xe1 0x83 0xb7	GEORGIAN SMALL LETTER YN
					*p = 0x1CB7;	//	Ჷ	0xe1 0xb2 0xb7	GEORGIAN CAPITAL LETTER YN
					break;
				case 0x10F8:		//	ჸ	0xe1 0x83 0xb8	GEORGIAN SMALL LETTER ELIFI
					*p = 0x1CB8;	//	Ჸ	0xe1 0xb2 0xb8	GEORGIAN CAPITAL LETTER ELIFI
					break;
				case 0x10F9:		//	ჹ	0xe1 0x83 0xb9	GEORGIAN SMALL LETTER TURNED GAN
					*p = 0x1CB9;	//	Ჹ	0xe1 0xb2 0xb9	GEORGIAN CAPITAL LETTER TURNED GAN
					break;
				case 0x10FA:		//	ჺ	0xe1 0x83 0xba	GEORGIAN SMALL LETTER AIN
					*p = 0x1CBA;	//	Ჺ	0xe1 0xb2 0xba	GEORGIAN CAPITAL LETTER AIN
					break;
				case 0x10FD:		//	ჽ	0xe1 0x83 0xbd	GEORGIAN SMALL LETTER AEN
					*p = 0x1CBD;	//	Ჽ	0xe1 0xb2 0xbd	GEORGIAN CAPITAL LETTER AEN
					break;
				case 0x10FE:		//	ჾ	0xe1 0x83 0xbe	GEORGIAN SMALL LETTER HARD SIGN
					*p = 0x1CBE;	//	Ჾ	0xe1 0xb2 0xbe	GEORGIAN CAPITAL LETTER HARD SIGN
					break;
				case 0x10FF:		//	ჿ	0xe1 0x83 0xbf	GEORGIAN SMALL LETTER LABIAL SIGN
					*p = 0x1CBF;	//	Ჿ	0xe1 0xb2 0xbf	GEORGIAN CAPITAL LETTER LABIAL SIGN
					break;
				case 0x13F8:		//	ᏸ	0xe1 0x8f 0xb8	CHEROKEE SMALL LETTER YE
					*p = 0x13F0;	//	Ᏸ	0xe1 0x8f 0xb0	CHEROKEE CAPITAL LETTER YE
					break;
				case 0x13F9:		//	ᏹ	0xe1 0x8f 0xb9	CHEROKEE SMALL LETTER YI
					*p = 0x13F1;	//	Ᏹ	0xe1 0x8f 0xb1	CHEROKEE CAPITAL LETTER YI
					break;
				case 0x13FA:		//	ᏺ	0xe1 0x8f 0xba	CHEROKEE SMALL LETTER YO
					*p = 0x13F2;	//	Ᏺ	0xe1 0x8f 0xb2	CHEROKEE CAPITAL LETTER YO
					break;
				case 0x13FB:		//	ᏻ	0xe1 0x8f 0xbb	CHEROKEE SMALL LETTER YU
					*p = 0x13F3;	//	Ᏻ	0xe1 0x8f 0xb3	CHEROKEE CAPITAL LETTER YU
					break;
				case 0x13FC:		//	ᏼ	0xe1 0x8f 0xbc	CHEROKEE SMALL LETTER YV
					*p = 0x13F4;	//	Ᏼ	0xe1 0x8f 0xb4	CHEROKEE CAPITAL LETTER YV
					break;
				case 0x13FD:		//	ᏽ	0xe1 0x8f 0xbd	CHEROKEE SMALL LETTER MV
					*p = 0x13F5;	//	Ᏽ	0xe1 0x8f 0xb5	CHEROKEE CAPITAL LETTER MV
					break;
				case 0x1D79:		//	ᵹ	0xe1 0xb5 0xb9	LATIN SMALL LETTER INSULAR G
					*p = 0xA77D;	//	Ᵹ	0xea 0x9d 0xbd	LATIN CAPITAL LETTER INSULAR G
					break;
				case 0x1D7D:		//	ᵽ	0xe1 0xb5 0xbd	LATIN SMALL LETTER P WITH STROKE
					*p = 0x2C63;	//	Ᵽ	0xe2 0xb1 0xa3	LATIN CAPITAL LETTER P WITH STROKE
					break;
				case 0x1D8E:		//	ᶎ	0xe1 0xb6 0x8e	LATIN SMALL LETTER Z WITH PALATAL HOOK
					*p = 0xA7C6;	//	Ᶎ	0xea 0x9f 0x86	LATIN CAPITAL LETTER Z WITH PALATAL HOOK
					break;
				case 0x1E01:		//	ḁ	0xe1 0xb8 0x81	LATIN SMALL LETTER A WITH RING BELOW
					*p = 0x1E00;	//	Ḁ	0xe1 0xb8 0x80	LATIN CAPITAL LETTER A WITH RING BELOW
					break;
				case 0x1E03:		//	ḃ	0xe1 0xb8 0x83	LATIN SMALL LETTER B WITH DOT ABOVE
					*p = 0x1E02;	//	Ḃ	0xe1 0xb8 0x82	LATIN CAPITAL LETTER B WITH DOT ABOVE
					break;
				case 0x1E05:		//	ḅ	0xe1 0xb8 0x85	LATIN SMALL LETTER B WITH DOT BELOW
					*p = 0x1E04;	//	Ḅ	0xe1 0xb8 0x84	LATIN CAPITAL LETTER B WITH DOT BELOW
					break;
				case 0x1E07:		//	ḇ	0xe1 0xb8 0x87	LATIN SMALL LETTER B WITH LINE BELOW
					*p = 0x1E06;	//	Ḇ	0xe1 0xb8 0x86	LATIN CAPITAL LETTER B WITH LINE BELOW
					break;
				case 0x1E09:		//	ḉ	0xe1 0xb8 0x89	LATIN SMALL LETTER C WITH CEDILLA AND ACUTE
					*p = 0x1E08;	//	Ḉ	0xe1 0xb8 0x88	LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE
					break;
				case 0x1E0B:		//	ḋ	0xe1 0xb8 0x8b	LATIN SMALL LETTER D WITH DOT ABOVE
					*p = 0x1E0A;	//	Ḋ	0xe1 0xb8 0x8a	LATIN CAPITAL LETTER D WITH DOT ABOVE
					break;
				case 0x1E0D:		//	ḍ	0xe1 0xb8 0x8d	LATIN SMALL LETTER D WITH DOT BELOW
					*p = 0x1E0C;	//	Ḍ	0xe1 0xb8 0x8c	LATIN CAPITAL LETTER D WITH DOT BELOW
					break;
				case 0x1E0F:		//	ḏ	0xe1 0xb8 0x8f	LATIN SMALL LETTER D WITH LINE BELOW
					*p = 0x1E0E;	//	Ḏ	0xe1 0xb8 0x8e	LATIN CAPITAL LETTER D WITH LINE BELOW
					break;
				case 0x1E11:		//	ḑ	0xe1 0xb8 0x91	LATIN SMALL LETTER D WITH CEDILLA
					*p = 0x1E10;	//	Ḑ	0xe1 0xb8 0x90	LATIN CAPITAL LETTER D WITH CEDILLA
					break;
				case 0x1E13:		//	ḓ	0xe1 0xb8 0x93	LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW
					*p = 0x1E12;	//	Ḓ	0xe1 0xb8 0x92	LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW
					break;
				case 0x1E15:		//	ḕ	0xe1 0xb8 0x95	LATIN SMALL LETTER E WITH MACRON AND GRAVE
					*p = 0x1E14;	//	Ḕ	0xe1 0xb8 0x94	LATIN CAPITAL LETTER E WITH MACRON AND GRAVE
					break;
				case 0x1E17:		//	ḗ	0xe1 0xb8 0x97	LATIN SMALL LETTER E WITH MACRON AND ACUTE
					*p = 0x1E16;	//	Ḗ	0xe1 0xb8 0x96	LATIN CAPITAL LETTER E WITH MACRON AND ACUTE
					break;
				case 0x1E19:		//	ḙ	0xe1 0xb8 0x99	LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW
					*p = 0x1E18;	//	Ḙ	0xe1 0xb8 0x98	LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW
					break;
				case 0x1E1B:		//	ḛ	0xe1 0xb8 0x9b	LATIN SMALL LETTER E WITH TILDE BELOW
					*p = 0x1E1A;	//	Ḛ	0xe1 0xb8 0x9a	LATIN CAPITAL LETTER E WITH TILDE BELOW
					break;
				case 0x1E1D:		//	ḝ	0xe1 0xb8 0x9d	LATIN SMALL LETTER E WITH CEDILLA AND BREVE
					*p = 0x1E1C;	//	Ḝ	0xe1 0xb8 0x9c	LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE
					break;
				case 0x1E1F:		//	ḟ	0xe1 0xb8 0x9f	LATIN SMALL LETTER F WITH DOT ABOVE
					*p = 0x1E1E;	//	Ḟ	0xe1 0xb8 0x9e	LATIN CAPITAL LETTER F WITH DOT ABOVE
					break;
				case 0x1E21:		//	ḡ	0xe1 0xb8 0xa1	LATIN SMALL LETTER G WITH MACRON
					*p = 0x1E20;	//	Ḡ	0xe1 0xb8 0xa0	LATIN CAPITAL LETTER G WITH MACRON
					break;
				case 0x1E23:		//	ḣ	0xe1 0xb8 0xa3	LATIN SMALL LETTER H WITH DOT ABOVE
					*p = 0x1E22;	//	Ḣ	0xe1 0xb8 0xa2	LATIN CAPITAL LETTER H WITH DOT ABOVE
					break;
				case 0x1E25:		//	ḥ	0xe1 0xb8 0xa5	LATIN SMALL LETTER H WITH DOT BELOW
					*p = 0x1E24;	//	Ḥ	0xe1 0xb8 0xa4	LATIN CAPITAL LETTER H WITH DOT BELOW
					break;
				case 0x1E27:		//	ḧ	0xe1 0xb8 0xa7	LATIN SMALL LETTER H WITH DIAERESIS
					*p = 0x1E26;	//	Ḧ	0xe1 0xb8 0xa6	LATIN CAPITAL LETTER H WITH DIAERESIS
					break;
				case 0x1E29:		//	ḩ	0xe1 0xb8 0xa9	LATIN SMALL LETTER H WITH CEDILLA
					*p = 0x1E28;	//	Ḩ	0xe1 0xb8 0xa8	LATIN CAPITAL LETTER H WITH CEDILLA
					break;
				case 0x1E2B:		//	ḫ	0xe1 0xb8 0xab	LATIN SMALL LETTER H WITH BREVE BELOW
					*p = 0x1E2A;	//	Ḫ	0xe1 0xb8 0xaa	LATIN CAPITAL LETTER H WITH BREVE BELOW
					break;
				case 0x1E2D:		//	ḭ	0xe1 0xb8 0xad	LATIN SMALL LETTER I WITH TILDE BELOW
					*p = 0x1E2C;	//	Ḭ	0xe1 0xb8 0xac	LATIN CAPITAL LETTER I WITH TILDE BELOW
					break;
				case 0x1E2F:		//	ḯ	0xe1 0xb8 0xaf	LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE
					*p = 0x1E2E;	//	Ḯ	0xe1 0xb8 0xae	LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE
					break;
				case 0x1E31:		//	ḱ	0xe1 0xb8 0xb1	LATIN SMALL LETTER K WITH ACUTE
					*p = 0x1E30;	//	Ḱ	0xe1 0xb8 0xb0	LATIN CAPITAL LETTER K WITH ACUTE
					break;
				case 0x1E33:		//	ḳ	0xe1 0xb8 0xb3	LATIN SMALL LETTER K WITH DOT BELOW
					*p = 0x1E32;	//	Ḳ	0xe1 0xb8 0xb2	LATIN CAPITAL LETTER K WITH DOT BELOW
					break;
				case 0x1E35:		//	ḵ	0xe1 0xb8 0xb5	LATIN SMALL LETTER K WITH LINE BELOW
					*p = 0x1E34;	//	Ḵ	0xe1 0xb8 0xb4	LATIN CAPITAL LETTER K WITH LINE BELOW
					break;
				case 0x1E37:		//	ḷ	0xe1 0xb8 0xb7	LATIN SMALL LETTER L WITH DOT BELOW
					*p = 0x1E36;	//	Ḷ	0xe1 0xb8 0xb6	LATIN CAPITAL LETTER L WITH DOT BELOW
					break;
				case 0x1E39:		//	ḹ	0xe1 0xb8 0xb9	LATIN SMALL LETTER L WITH DOT BELOW AND MACRON
					*p = 0x1E38;	//	Ḹ	0xe1 0xb8 0xb8	LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON
					break;
				case 0x1E3B:		//	ḻ	0xe1 0xb8 0xbb	LATIN SMALL LETTER L WITH LINE BELOW
					*p = 0x1E3A;	//	Ḻ	0xe1 0xb8 0xba	LATIN CAPITAL LETTER L WITH LINE BELOW
					break;
				case 0x1E3D:		//	ḽ	0xe1 0xb8 0xbd	LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW
					*p = 0x1E3C;	//	Ḽ	0xe1 0xb8 0xbc	LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW
					break;
				case 0x1E3F:		//	ḿ	0xe1 0xb8 0xbf	LATIN SMALL LETTER M WITH ACUTE
					*p = 0x1E3E;	//	Ḿ	0xe1 0xb8 0xbe	LATIN CAPITAL LETTER M WITH ACUTE
					break;
				case 0x1E41:		//	ṁ	0xe1 0xb9 0x81	LATIN SMALL LETTER M WITH DOT ABOVE
					*p = 0x1E40;	//	Ṁ	0xe1 0xb9 0x80	LATIN CAPITAL LETTER M WITH DOT ABOVE
					break;
				case 0x1E43:		//	ṃ	0xe1 0xb9 0x83	LATIN SMALL LETTER M WITH DOT BELOW
					*p = 0x1E42;	//	Ṃ	0xe1 0xb9 0x82	LATIN CAPITAL LETTER M WITH DOT BELOW
					break;
				case 0x1E45:		//	ṅ	0xe1 0xb9 0x85	LATIN SMALL LETTER N WITH DOT ABOVE
					*p = 0x1E44;	//	Ṅ	0xe1 0xb9 0x84	LATIN CAPITAL LETTER N WITH DOT ABOVE
					break;
				case 0x1E47:		//	ṇ	0xe1 0xb9 0x87	LATIN SMALL LETTER N WITH DOT BELOW
					*p = 0x1E46;	//	Ṇ	0xe1 0xb9 0x86	LATIN CAPITAL LETTER N WITH DOT BELOW
					break;
				case 0x1E49:		//	ṉ	0xe1 0xb9 0x89	LATIN SMALL LETTER N WITH LINE BELOW
					*p = 0x1E48;	//	Ṉ	0xe1 0xb9 0x88	LATIN CAPITAL LETTER N WITH LINE BELOW
					break;
				case 0x1E4B:		//	ṋ	0xe1 0xb9 0x8b	LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW
					*p = 0x1E4A;	//	Ṋ	0xe1 0xb9 0x8a	LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW
					break;
				case 0x1E4D:		//	ṍ	0xe1 0xb9 0x8d	LATIN SMALL LETTER O WITH TILDE AND ACUTE
					*p = 0x1E4C;	//	Ṍ	0xe1 0xb9 0x8c	LATIN CAPITAL LETTER O WITH TILDE AND ACUTE
					break;
				case 0x1E4F:		//	ṏ	0xe1 0xb9 0x8f	LATIN SMALL LETTER O WITH TILDE AND DIAERESIS
					*p = 0x1E4E;	//	Ṏ	0xe1 0xb9 0x8e	LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS
					break;
				case 0x1E51:		//	ṑ	0xe1 0xb9 0x91	LATIN SMALL LETTER O WITH MACRON AND GRAVE
					*p = 0x1E50;	//	Ṑ	0xe1 0xb9 0x90	LATIN CAPITAL LETTER O WITH MACRON AND GRAVE
					break;
				case 0x1E53:		//	ṓ	0xe1 0xb9 0x93	LATIN SMALL LETTER O WITH MACRON AND ACUTE
					*p = 0x1E52;	//	Ṓ	0xe1 0xb9 0x92	LATIN CAPITAL LETTER O WITH MACRON AND ACUTE
					break;
				case 0x1E55:		//	ṕ	0xe1 0xb9 0x95	LATIN SMALL LETTER P WITH ACUTE
					*p = 0x1E54;	//	Ṕ	0xe1 0xb9 0x94	LATIN CAPITAL LETTER P WITH ACUTE
					break;
				case 0x1E57:		//	ṗ	0xe1 0xb9 0x97	LATIN SMALL LETTER P WITH DOT ABOVE
					*p = 0x1E56;	//	Ṗ	0xe1 0xb9 0x96	LATIN CAPITAL LETTER P WITH DOT ABOVE
					break;
				case 0x1E59:		//	ṙ	0xe1 0xb9 0x99	LATIN SMALL LETTER R WITH DOT ABOVE
					*p = 0x1E58;	//	Ṙ	0xe1 0xb9 0x98	LATIN CAPITAL LETTER R WITH DOT ABOVE
					break;
				case 0x1E5B:		//	ṛ	0xe1 0xb9 0x9b	LATIN SMALL LETTER R WITH DOT BELOW
					*p = 0x1E5A;	//	Ṛ	0xe1 0xb9 0x9a	LATIN CAPITAL LETTER R WITH DOT BELOW
					break;
				case 0x1E5D:		//	ṝ	0xe1 0xb9 0x9d	LATIN SMALL LETTER R WITH DOT BELOW AND MACRON
					*p = 0x1E5C;	//	Ṝ	0xe1 0xb9 0x9c	LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON
					break;
				case 0x1E5F:		//	ṟ	0xe1 0xb9 0x9f	LATIN SMALL LETTER R WITH LINE BELOW
					*p = 0x1E5E;	//	Ṟ	0xe1 0xb9 0x9e	LATIN CAPITAL LETTER R WITH LINE BELOW
					break;
				case 0x1E61:		//	ṡ	0xe1 0xb9 0xa1	LATIN SMALL LETTER S WITH DOT ABOVE
					*p = 0x1E60;	//	Ṡ	0xe1 0xb9 0xa0	LATIN CAPITAL LETTER S WITH DOT ABOVE
					break;
				case 0x1E63:		//	ṣ	0xe1 0xb9 0xa3	LATIN SMALL LETTER S WITH DOT BELOW
					*p = 0x1E62;	//	Ṣ	0xe1 0xb9 0xa2	LATIN CAPITAL LETTER S WITH DOT BELOW
					break;
				case 0x1E65:		//	ṥ	0xe1 0xb9 0xa5	LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE
					*p = 0x1E64;	//	Ṥ	0xe1 0xb9 0xa4	LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE
					break;
				case 0x1E67:		//	ṧ	0xe1 0xb9 0xa7	LATIN SMALL LETTER S WITH CARON AND DOT ABOVE
					*p = 0x1E66;	//	Ṧ	0xe1 0xb9 0xa6	LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE
					break;
				case 0x1E69:		//	ṩ	0xe1 0xb9 0xa9	LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE
					*p = 0x1E68;	//	Ṩ	0xe1 0xb9 0xa8	LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE
					break;
				case 0x1E6B:		//	ṫ	0xe1 0xb9 0xab	LATIN SMALL LETTER T WITH DOT ABOVE
					*p = 0x1E6A;	//	Ṫ	0xe1 0xb9 0xaa	LATIN CAPITAL LETTER T WITH DOT ABOVE
					break;
				case 0x1E6D:		//	ṭ	0xe1 0xb9 0xad	LATIN SMALL LETTER T WITH DOT BELOW
					*p = 0x1E6C;	//	Ṭ	0xe1 0xb9 0xac	LATIN CAPITAL LETTER T WITH DOT BELOW
					break;
				case 0x1E6F:		//	ṯ	0xe1 0xb9 0xaf	LATIN SMALL LETTER T WITH LINE BELOW
					*p = 0x1E6E;	//	Ṯ	0xe1 0xb9 0xae	LATIN CAPITAL LETTER T WITH LINE BELOW
					break;
				case 0x1E71:		//	ṱ	0xe1 0xb9 0xb1	LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW
					*p = 0x1E70;	//	Ṱ	0xe1 0xb9 0xb0	LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW
					break;
				case 0x1E73:		//	ṳ	0xe1 0xb9 0xb3	LATIN SMALL LETTER U WITH DIAERESIS BELOW
					*p = 0x1E72;	//	Ṳ	0xe1 0xb9 0xb2	LATIN CAPITAL LETTER U WITH DIAERESIS BELOW
					break;
				case 0x1E75:		//	ṵ	0xe1 0xb9 0xb5	LATIN SMALL LETTER U WITH TILDE BELOW
					*p = 0x1E74;	//	Ṵ	0xe1 0xb9 0xb4	LATIN CAPITAL LETTER U WITH TILDE BELOW
					break;
				case 0x1E77:		//	ṷ	0xe1 0xb9 0xb7	LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW
					*p = 0x1E76;	//	Ṷ	0xe1 0xb9 0xb6	LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW
					break;
				case 0x1E79:		//	ṹ	0xe1 0xb9 0xb9	LATIN SMALL LETTER U WITH TILDE AND ACUTE
					*p = 0x1E78;	//	Ṹ	0xe1 0xb9 0xb8	LATIN CAPITAL LETTER U WITH TILDE AND ACUTE
					break;
				case 0x1E7B:		//	ṻ	0xe1 0xb9 0xbb	LATIN SMALL LETTER U WITH MACRON AND DIAERESIS
					*p = 0x1E7A;	//	Ṻ	0xe1 0xb9 0xba	LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS
					break;
				case 0x1E7D:		//	ṽ	0xe1 0xb9 0xbd	LATIN SMALL LETTER V WITH TILDE
					*p = 0x1E7C;	//	Ṽ	0xe1 0xb9 0xbc	LATIN CAPITAL LETTER V WITH TILDE
					break;
				case 0x1E7F:		//	ṿ	0xe1 0xb9 0xbf	LATIN SMALL LETTER V WITH DOT BELOW
					*p = 0x1E7E;	//	Ṿ	0xe1 0xb9 0xbe	LATIN CAPITAL LETTER V WITH DOT BELOW
					break;
				case 0x1E81:		//	ẁ	0xe1 0xba 0x81	LATIN SMALL LETTER W WITH GRAVE
					*p = 0x1E80;	//	Ẁ	0xe1 0xba 0x80	LATIN CAPITAL LETTER W WITH GRAVE
					break;
				case 0x1E83:		//	ẃ	0xe1 0xba 0x83	LATIN SMALL LETTER W WITH ACUTE
					*p = 0x1E82;	//	Ẃ	0xe1 0xba 0x82	LATIN CAPITAL LETTER W WITH ACUTE
					break;
				case 0x1E85:		//	ẅ	0xe1 0xba 0x85	LATIN SMALL LETTER W WITH DIAERESIS
					*p = 0x1E84;	//	Ẅ	0xe1 0xba 0x84	LATIN CAPITAL LETTER W WITH DIAERESIS
					break;
				case 0x1E87:		//	ẇ	0xe1 0xba 0x87	LATIN SMALL LETTER W WITH DOT ABOVE
					*p = 0x1E86;	//	Ẇ	0xe1 0xba 0x86	LATIN CAPITAL LETTER W WITH DOT ABOVE
					break;
				case 0x1E89:		//	ẉ	0xe1 0xba 0x89	LATIN SMALL LETTER W WITH DOT BELOW
					*p = 0x1E88;	//	Ẉ	0xe1 0xba 0x88	LATIN CAPITAL LETTER W WITH DOT BELOW
					break;
				case 0x1E8B:		//	ẋ	0xe1 0xba 0x8b	LATIN SMALL LETTER X WITH DOT ABOVE
					*p = 0x1E8A;	//	Ẋ	0xe1 0xba 0x8a	LATIN CAPITAL LETTER X WITH DOT ABOVE
					break;
				case 0x1E8D:		//	ẍ	0xe1 0xba 0x8d	LATIN SMALL LETTER X WITH DIAERESIS
					*p = 0x1E8C;	//	Ẍ	0xe1 0xba 0x8c	LATIN CAPITAL LETTER X WITH DIAERESIS
					break;
				case 0x1E8F:		//	ẏ	0xe1 0xba 0x8f	LATIN SMALL LETTER Y WITH DOT ABOVE
					*p = 0x1E8E;	//	Ẏ	0xe1 0xba 0x8e	LATIN CAPITAL LETTER Y WITH DOT ABOVE
					break;
				case 0x1E91:		//	ẑ	0xe1 0xba 0x91	LATIN SMALL LETTER Z WITH CIRCUMFLEX
					*p = 0x1E90;	//	Ẑ	0xe1 0xba 0x90	LATIN CAPITAL LETTER Z WITH CIRCUMFLEX
					break;
				case 0x1E93:		//	ẓ	0xe1 0xba 0x93	LATIN SMALL LETTER Z WITH DOT BELOW
					*p = 0x1E92;	//	Ẓ	0xe1 0xba 0x92	LATIN CAPITAL LETTER Z WITH DOT BELOW
					break;
				case 0x1E95:		//	ẕ	0xe1 0xba 0x95	LATIN SMALL LETTER Z WITH LINE BELOW
					*p = 0x1E94;	//	Ẕ	0xe1 0xba 0x94	LATIN CAPITAL LETTER Z WITH LINE BELOW
					break;
				case 0x1EA1:		//	ạ	0xe1 0xba 0xa1	LATIN SMALL LETTER A WITH DOT BELOW
					*p = 0x1EA0;	//	Ạ	0xe1 0xba 0xa0	LATIN CAPITAL LETTER A WITH DOT BELOW
					break;
				case 0x1EA3:		//	ả	0xe1 0xba 0xa3	LATIN SMALL LETTER A WITH HOOK ABOVE
					*p = 0x1EA2;	//	Ả	0xe1 0xba 0xa2	LATIN CAPITAL LETTER A WITH HOOK ABOVE
					break;
				case 0x1EA5:		//	ấ	0xe1 0xba 0xa5	LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE
					*p = 0x1EA4;	//	Ấ	0xe1 0xba 0xa4	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE
					break;
				case 0x1EA7:		//	ầ	0xe1 0xba 0xa7	LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE
					*p = 0x1EA6;	//	Ầ	0xe1 0xba 0xa6	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE
					break;
				case 0x1EA9:		//	ẩ	0xe1 0xba 0xa9	LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE
					*p = 0x1EA8;	//	Ẩ	0xe1 0xba 0xa8	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE
					break;
				case 0x1EAB:		//	ẫ	0xe1 0xba 0xab	LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE
					*p = 0x1EAA;	//	Ẫ	0xe1 0xba 0xaa	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE
					break;
				case 0x1EAD:		//	ậ	0xe1 0xba 0xad	LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW
					*p = 0x1EAC;	//	Ậ	0xe1 0xba 0xac	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW
					break;
				case 0x1EAF:		//	ắ	0xe1 0xba 0xaf	LATIN SMALL LETTER A WITH BREVE AND ACUTE
					*p = 0x1EAE;	//	Ắ	0xe1 0xba 0xae	LATIN CAPITAL LETTER A WITH BREVE AND ACUTE
					break;
				case 0x1EB1:		//	ằ	0xe1 0xba 0xb1	LATIN SMALL LETTER A WITH BREVE AND GRAVE
					*p = 0x1EB0;	//	Ằ	0xe1 0xba 0xb0	LATIN CAPITAL LETTER A WITH BREVE AND GRAVE
					break;
				case 0x1EB3:		//	ẳ	0xe1 0xba 0xb3	LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE
					*p = 0x1EB2;	//	Ẳ	0xe1 0xba 0xb2	LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE
					break;
				case 0x1EB5:		//	ẵ	0xe1 0xba 0xb5	LATIN SMALL LETTER A WITH BREVE AND TILDE
					*p = 0x1EB4;	//	Ẵ	0xe1 0xba 0xb4	LATIN CAPITAL LETTER A WITH BREVE AND TILDE
					break;
				case 0x1EB7:		//	ặ	0xe1 0xba 0xb7	LATIN SMALL LETTER A WITH BREVE AND DOT BELOW
					*p = 0x1EB6;	//	Ặ	0xe1 0xba 0xb6	LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW
					break;
				case 0x1EB9:		//	ẹ	0xe1 0xba 0xb9	LATIN SMALL LETTER E WITH DOT BELOW
					*p = 0x1EB8;	//	Ẹ	0xe1 0xba 0xb8	LATIN CAPITAL LETTER E WITH DOT BELOW
					break;
				case 0x1EBB:		//	ẻ	0xe1 0xba 0xbb	LATIN SMALL LETTER E WITH HOOK ABOVE
					*p = 0x1EBA;	//	Ẻ	0xe1 0xba 0xba	LATIN CAPITAL LETTER E WITH HOOK ABOVE
					break;
				case 0x1EBD:		//	ẽ	0xe1 0xba 0xbd	LATIN SMALL LETTER E WITH TILDE
					*p = 0x1EBC;	//	Ẽ	0xe1 0xba 0xbc	LATIN CAPITAL LETTER E WITH TILDE
					break;
				case 0x1EBF:		//	ế	0xe1 0xba 0xbf	LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE
					*p = 0x1EBE;	//	Ế	0xe1 0xba 0xbe	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE
					break;
				case 0x1EC1:		//	ề	0xe1 0xbb 0x81	LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE
					*p = 0x1EC0;	//	Ề	0xe1 0xbb 0x80	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE
					break;
				case 0x1EC3:		//	ể	0xe1 0xbb 0x83	LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE
					*p = 0x1EC2;	//	Ể	0xe1 0xbb 0x82	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE
					break;
				case 0x1EC5:		//	ễ	0xe1 0xbb 0x85	LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE
					*p = 0x1EC4;	//	Ễ	0xe1 0xbb 0x84	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE
					break;
				case 0x1EC7:		//	ệ	0xe1 0xbb 0x87	LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW
					*p = 0x1EC6;	//	Ệ	0xe1 0xbb 0x86	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW
					break;
				case 0x1EC9:		//	ỉ	0xe1 0xbb 0x89	LATIN SMALL LETTER I WITH HOOK ABOVE
					*p = 0x1EC8;	//	Ỉ	0xe1 0xbb 0x88	LATIN CAPITAL LETTER I WITH HOOK ABOVE
					break;
				case 0x1ECB:		//	ị	0xe1 0xbb 0x8b	LATIN SMALL LETTER I WITH DOT BELOW
					*p = 0x1ECA;	//	Ị	0xe1 0xbb 0x8a	LATIN CAPITAL LETTER I WITH DOT BELOW
					break;
				case 0x1ECD:		//	ọ	0xe1 0xbb 0x8d	LATIN SMALL LETTER O WITH DOT BELOW
					*p = 0x1ECC;	//	Ọ	0xe1 0xbb 0x8c	LATIN CAPITAL LETTER O WITH DOT BELOW
					break;
				case 0x1ECF:		//	ỏ	0xe1 0xbb 0x8f	LATIN SMALL LETTER O WITH HOOK ABOVE
					*p = 0x1ECE;	//	Ỏ	0xe1 0xbb 0x8e	LATIN CAPITAL LETTER O WITH HOOK ABOVE
					break;
				case 0x1ED1:		//	ố	0xe1 0xbb 0x91	LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE
					*p = 0x1ED0;	//	Ố	0xe1 0xbb 0x90	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE
					break;
				case 0x1ED3:		//	ồ	0xe1 0xbb 0x93	LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE
					*p = 0x1ED2;	//	Ồ	0xe1 0xbb 0x92	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE
					break;
				case 0x1ED5:		//	ổ	0xe1 0xbb 0x95	LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE
					*p = 0x1ED4;	//	Ổ	0xe1 0xbb 0x94	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE
					break;
				case 0x1ED7:		//	ỗ	0xe1 0xbb 0x97	LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE
					*p = 0x1ED6;	//	Ỗ	0xe1 0xbb 0x96	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE
					break;
				case 0x1ED9:		//	ộ	0xe1 0xbb 0x99	LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW
					*p = 0x1ED8;	//	Ộ	0xe1 0xbb 0x98	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW
					break;
				case 0x1EDB:		//	ớ	0xe1 0xbb 0x9b	LATIN SMALL LETTER O WITH HORN AND ACUTE
					*p = 0x1EDA;	//	Ớ	0xe1 0xbb 0x9a	LATIN CAPITAL LETTER O WITH HORN AND ACUTE
					break;
				case 0x1EDD:		//	ờ	0xe1 0xbb 0x9d	LATIN SMALL LETTER O WITH HORN AND GRAVE
					*p = 0x1EDC;	//	Ờ	0xe1 0xbb 0x9c	LATIN CAPITAL LETTER O WITH HORN AND GRAVE
					break;
				case 0x1EDF:		//	ở	0xe1 0xbb 0x9f	LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE
					*p = 0x1EDE;	//	Ở	0xe1 0xbb 0x9e	LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE
					break;
				case 0x1EE1:		//	ỡ	0xe1 0xbb 0xa1	LATIN SMALL LETTER O WITH HORN AND TILDE
					*p = 0x1EE0;	//	Ỡ	0xe1 0xbb 0xa0	LATIN CAPITAL LETTER O WITH HORN AND TILDE
					break;
				case 0x1EE3:		//	ợ	0xe1 0xbb 0xa3	LATIN SMALL LETTER O WITH HORN AND DOT BELOW
					*p = 0x1EE2;	//	Ợ	0xe1 0xbb 0xa2	LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW
					break;
				case 0x1EE5:		//	ụ	0xe1 0xbb 0xa5	LATIN SMALL LETTER U WITH DOT BELOW
					*p = 0x1EE4;	//	Ụ	0xe1 0xbb 0xa4	LATIN CAPITAL LETTER U WITH DOT BELOW
					break;
				case 0x1EE7:		//	ủ	0xe1 0xbb 0xa7	LATIN SMALL LETTER U WITH HOOK ABOVE
					*p = 0x1EE6;	//	Ủ	0xe1 0xbb 0xa6	LATIN CAPITAL LETTER U WITH HOOK ABOVE
					break;
				case 0x1EE9:		//	ứ	0xe1 0xbb 0xa9	LATIN SMALL LETTER U WITH HORN AND ACUTE
					*p = 0x1EE8;	//	Ứ	0xe1 0xbb 0xa8	LATIN CAPITAL LETTER U WITH HORN AND ACUTE
					break;
				case 0x1EEB:		//	ừ	0xe1 0xbb 0xab	LATIN SMALL LETTER U WITH HORN AND GRAVE
					*p = 0x1EEA;	//	Ừ	0xe1 0xbb 0xaa	LATIN CAPITAL LETTER U WITH HORN AND GRAVE
					break;
				case 0x1EED:		//	ử	0xe1 0xbb 0xad	LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE
					*p = 0x1EEC;	//	Ử	0xe1 0xbb 0xac	LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE
					break;
				case 0x1EEF:		//	ữ	0xe1 0xbb 0xaf	LATIN SMALL LETTER U WITH HORN AND TILDE
					*p = 0x1EEE;	//	Ữ	0xe1 0xbb 0xae	LATIN CAPITAL LETTER U WITH HORN AND TILDE
					break;
				case 0x1EF1:		//	ự	0xe1 0xbb 0xb1	LATIN SMALL LETTER U WITH HORN AND DOT BELOW
					*p = 0x1EF0;	//	Ự	0xe1 0xbb 0xb0	LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW
					break;
				case 0x1EF3:		//	ỳ	0xe1 0xbb 0xb3	LATIN SMALL LETTER Y WITH GRAVE
					*p = 0x1EF2;	//	Ỳ	0xe1 0xbb 0xb2	LATIN CAPITAL LETTER Y WITH GRAVE
					break;
				case 0x1EF5:		//	ỵ	0xe1 0xbb 0xb5	LATIN SMALL LETTER Y WITH DOT BELOW
					*p = 0x1EF4;	//	Ỵ	0xe1 0xbb 0xb4	LATIN CAPITAL LETTER Y WITH DOT BELOW
					break;
				case 0x1EF7:		//	ỷ	0xe1 0xbb 0xb7	LATIN SMALL LETTER Y WITH HOOK ABOVE
					*p = 0x1EF6;	//	Ỷ	0xe1 0xbb 0xb6	LATIN CAPITAL LETTER Y WITH HOOK ABOVE
					break;
				case 0x1EF9:		//	ỹ	0xe1 0xbb 0xb9	LATIN SMALL LETTER Y WITH TILDE
					*p = 0x1EF8;	//	Ỹ	0xe1 0xbb 0xb8	LATIN CAPITAL LETTER Y WITH TILDE
					break;
				case 0x1EFB:		//	ỻ	0xe1 0xbb 0xbb	LATIN SMALL LETTER MIDDLE-WELSH LL
					*p = 0x1EFA;	//	Ỻ	0xe1 0xbb 0xba	LATIN CAPITAL LETTER MIDDLE-WELSH LL
					break;
				case 0x1EFD:		//	ỽ	0xe1 0xbb 0xbd	LATIN SMALL LETTER MIDDLE-WELSH V
					*p = 0x1EFC;	//	Ỽ	0xe1 0xbb 0xbc	LATIN CAPITAL LETTER MIDDLE-WELSH V
					break;
				case 0x1EFF:		//	ỿ	0xe1 0xbb 0xbf	LATIN SMALL LETTER Y WITH LOOP
					*p = 0x1EFE;	//	Ỿ	0xe1 0xbb 0xbe	LATIN CAPITAL LETTER Y WITH LOOP
					break;
				case 0x1F00:		//	ἀ	0xe1 0xbc 0x80	GREEK SMALL LETTER ALPHA WITH PSILI
					*p = 0x1F08;	//	Ἀ	0xe1 0xbc 0x88	GREEK CAPITAL LETTER ALPHA WITH PSILI
					break;
				case 0x1F01:		//	ἁ	0xe1 0xbc 0x81	GREEK SMALL LETTER ALPHA WITH DASIA
					*p = 0x1F09;	//	Ἁ	0xe1 0xbc 0x89	GREEK CAPITAL LETTER ALPHA WITH DASIA
					break;
				case 0x1F02:		//	ἂ	0xe1 0xbc 0x82	GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA
					*p = 0x1F0A;	//	Ἂ	0xe1 0xbc 0x8a	GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA
					break;
				case 0x1F03:		//	ἃ	0xe1 0xbc 0x83	GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA
					*p = 0x1F0B;	//	Ἃ	0xe1 0xbc 0x8b	GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA
					break;
				case 0x1F04:		//	ἄ	0xe1 0xbc 0x84	GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA
					*p = 0x1F0C;	//	Ἄ	0xe1 0xbc 0x8c	GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA
					break;
				case 0x1F05:		//	ἅ	0xe1 0xbc 0x85	GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA
					*p = 0x1F0D;	//	Ἅ	0xe1 0xbc 0x8d	GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA
					break;
				case 0x1F06:		//	ἆ	0xe1 0xbc 0x86	GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI
					*p = 0x1F0E;	//	Ἆ	0xe1 0xbc 0x8e	GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F07:		//	ἇ	0xe1 0xbc 0x87	GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI
					*p = 0x1F0F;	//	Ἇ	0xe1 0xbc 0x8f	GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F10:		//	ἐ	0xe1 0xbc 0x90	GREEK SMALL LETTER EPSILON WITH PSILI
					*p = 0x1F18;	//	Ἐ	0xe1 0xbc 0x98	GREEK CAPITAL LETTER EPSILON WITH PSILI
					break;
				case 0x1F11:		//	ἑ	0xe1 0xbc 0x91	GREEK SMALL LETTER EPSILON WITH DASIA
					*p = 0x1F19;	//	Ἑ	0xe1 0xbc 0x99	GREEK CAPITAL LETTER EPSILON WITH DASIA
					break;
				case 0x1F12:		//	ἒ	0xe1 0xbc 0x92	GREEK SMALL LETTER EPSILON WITH PSILI AND VARIA
					*p = 0x1F1A;	//	Ἒ	0xe1 0xbc 0x9a	GREEK CAPITAL LETTER EPSILON WITH PSILI AND VARIA
					break;
				case 0x1F13:		//	ἓ	0xe1 0xbc 0x93	GREEK SMALL LETTER EPSILON WITH DASIA AND VARIA
					*p = 0x1F1B;	//	Ἓ	0xe1 0xbc 0x9b	GREEK CAPITAL LETTER EPSILON WITH DASIA AND VARIA
					break;
				case 0x1F14:		//	ἔ	0xe1 0xbc 0x94	GREEK SMALL LETTER EPSILON WITH PSILI AND OXIA
					*p = 0x1F1C;	//	Ἔ	0xe1 0xbc 0x9c	GREEK CAPITAL LETTER EPSILON WITH PSILI AND OXIA
					break;
				case 0x1F15:		//	ἕ	0xe1 0xbc 0x95	GREEK SMALL LETTER EPSILON WITH DASIA AND OXIA
					*p = 0x1F1D;	//	Ἕ	0xe1 0xbc 0x9d	GREEK CAPITAL LETTER EPSILON WITH DASIA AND OXIA
					break;
				case 0x1F20:		//	ἠ	0xe1 0xbc 0xa0	GREEK SMALL LETTER ETA WITH PSILI
					*p = 0x1F28;	//	Ἠ	0xe1 0xbc 0xa8	GREEK CAPITAL LETTER ETA WITH PSILI
					break;
				case 0x1F21:		//	ἡ	0xe1 0xbc 0xa1	GREEK SMALL LETTER ETA WITH DASIA
					*p = 0x1F29;	//	Ἡ	0xe1 0xbc 0xa9	GREEK CAPITAL LETTER ETA WITH DASIA
					break;
				case 0x1F22:		//	ἢ	0xe1 0xbc 0xa2	GREEK SMALL LETTER ETA WITH PSILI AND VARIA
					*p = 0x1F2A;	//	Ἢ	0xe1 0xbc 0xaa	GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA
					break;
				case 0x1F23:		//	ἣ	0xe1 0xbc 0xa3	GREEK SMALL LETTER ETA WITH DASIA AND VARIA
					*p = 0x1F2B;	//	Ἣ	0xe1 0xbc 0xab	GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA
					break;
				case 0x1F24:		//	ἤ	0xe1 0xbc 0xa4	GREEK SMALL LETTER ETA WITH PSILI AND OXIA
					*p = 0x1F2C;	//	Ἤ	0xe1 0xbc 0xac	GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA
					break;
				case 0x1F25:		//	ἥ	0xe1 0xbc 0xa5	GREEK SMALL LETTER ETA WITH DASIA AND OXIA
					*p = 0x1F2D;	//	Ἥ	0xe1 0xbc 0xad	GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA
					break;
				case 0x1F26:		//	ἦ	0xe1 0xbc 0xa6	GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI
					*p = 0x1F2E;	//	Ἦ	0xe1 0xbc 0xae	GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F27:		//	ἧ	0xe1 0xbc 0xa7	GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI
					*p = 0x1F2F;	//	Ἧ	0xe1 0xbc 0xaf	GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F30:		//	ἰ	0xe1 0xbc 0xb0	GREEK SMALL LETTER IOTA WITH PSILI
					*p = 0x1F38;	//	Ἰ	0xe1 0xbc 0xb8	GREEK CAPITAL LETTER IOTA WITH PSILI
					break;
				case 0x1F31:		//	ἱ	0xe1 0xbc 0xb1	GREEK SMALL LETTER IOTA WITH DASIA
					*p = 0x1F39;	//	Ἱ	0xe1 0xbc 0xb9	GREEK CAPITAL LETTER IOTA WITH DASIA
					break;
				case 0x1F32:		//	ἲ	0xe1 0xbc 0xb2	GREEK SMALL LETTER IOTA WITH PSILI AND VARIA
					*p = 0x1F3A;	//	Ἲ	0xe1 0xbc 0xba	GREEK CAPITAL LETTER IOTA WITH PSILI AND VARIA
					break;
				case 0x1F33:		//	ἳ	0xe1 0xbc 0xb3	GREEK SMALL LETTER IOTA WITH DASIA AND VARIA
					*p = 0x1F3B;	//	Ἳ	0xe1 0xbc 0xbb	GREEK CAPITAL LETTER IOTA WITH DASIA AND VARIA
					break;
				case 0x1F34:		//	ἴ	0xe1 0xbc 0xb4	GREEK SMALL LETTER IOTA WITH PSILI AND OXIA
					*p = 0x1F3C;	//	Ἴ	0xe1 0xbc 0xbc	GREEK CAPITAL LETTER IOTA WITH PSILI AND OXIA
					break;
				case 0x1F35:		//	ἵ	0xe1 0xbc 0xb5	GREEK SMALL LETTER IOTA WITH DASIA AND OXIA
					*p = 0x1F3D;	//	Ἵ	0xe1 0xbc 0xbd	GREEK CAPITAL LETTER IOTA WITH DASIA AND OXIA
					break;
				case 0x1F36:		//	ἶ	0xe1 0xbc 0xb6	GREEK SMALL LETTER IOTA WITH PSILI AND PERISPOMENI
					*p = 0x1F3E;	//	Ἶ	0xe1 0xbc 0xbe	GREEK CAPITAL LETTER IOTA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F37:		//	ἷ	0xe1 0xbc 0xb7	GREEK SMALL LETTER IOTA WITH DASIA AND PERISPOMENI
					*p = 0x1F3F;	//	Ἷ	0xe1 0xbc 0xbf	GREEK CAPITAL LETTER IOTA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F40:		//	ὀ	0xe1 0xbd 0x80	GREEK SMALL LETTER OMICRON WITH PSILI
					*p = 0x1F48;	//	Ὀ	0xe1 0xbd 0x88	GREEK CAPITAL LETTER OMICRON WITH PSILI
					break;
				case 0x1F41:		//	ὁ	0xe1 0xbd 0x81	GREEK SMALL LETTER OMICRON WITH DASIA
					*p = 0x1F49;	//	Ὁ	0xe1 0xbd 0x89	GREEK CAPITAL LETTER OMICRON WITH DASIA
					break;
				case 0x1F42:		//	ὂ	0xe1 0xbd 0x82	GREEK SMALL LETTER OMICRON WITH PSILI AND VARIA
					*p = 0x1F4A;	//	Ὂ	0xe1 0xbd 0x8a	GREEK CAPITAL LETTER OMICRON WITH PSILI AND VARIA
					break;
				case 0x1F43:		//	ὃ	0xe1 0xbd 0x83	GREEK SMALL LETTER OMICRON WITH DASIA AND VARIA
					*p = 0x1F4B;	//	Ὃ	0xe1 0xbd 0x8b	GREEK CAPITAL LETTER OMICRON WITH DASIA AND VARIA
					break;
				case 0x1F44:		//	ὄ	0xe1 0xbd 0x84	GREEK SMALL LETTER OMICRON WITH PSILI AND OXIA
					*p = 0x1F4C;	//	Ὄ	0xe1 0xbd 0x8c	GREEK CAPITAL LETTER OMICRON WITH PSILI AND OXIA
					break;
				case 0x1F45:		//	ὅ	0xe1 0xbd 0x85	GREEK SMALL LETTER OMICRON WITH DASIA AND OXIA
					*p = 0x1F4D;	//	Ὅ	0xe1 0xbd 0x8d	GREEK CAPITAL LETTER OMICRON WITH DASIA AND OXIA
					break;
				case 0x1F51:		//	ὑ	0xe1 0xbd 0x91	GREEK SMALL LETTER UPSILON WITH DASIA
					*p = 0x1F59;	//	Ὑ	0xe1 0xbd 0x99	GREEK CAPITAL LETTER UPSILON WITH DASIA
					break;
				case 0x1F53:		//	ὓ	0xe1 0xbd 0x93	GREEK SMALL LETTER UPSILON WITH DASIA AND VARIA
					*p = 0x1F5B;	//	Ὓ	0xe1 0xbd 0x9b	GREEK CAPITAL LETTER UPSILON WITH DASIA AND VARIA
					break;
				case 0x1F55:		//	ὕ	0xe1 0xbd 0x95	GREEK SMALL LETTER UPSILON WITH DASIA AND OXIA
					*p = 0x1F5D;	//	Ὕ	0xe1 0xbd 0x9d	GREEK CAPITAL LETTER UPSILON WITH DASIA AND OXIA
					break;
				case 0x1F57:		//	ὗ	0xe1 0xbd 0x97	GREEK SMALL LETTER UPSILON WITH DASIA AND PERISPOMENI
					*p = 0x1F5F;	//	Ὗ	0xe1 0xbd 0x9f	GREEK CAPITAL LETTER UPSILON WITH DASIA AND PERISPOMENI
					break;
				case 0x1F60:		//	ὠ	0xe1 0xbd 0xa0	GREEK SMALL LETTER OMEGA WITH PSILI
					*p = 0x1F68;	//	Ὠ	0xe1 0xbd 0xa8	GREEK CAPITAL LETTER OMEGA WITH PSILI
					break;
				case 0x1F61:		//	ὡ	0xe1 0xbd 0xa1	GREEK SMALL LETTER OMEGA WITH DASIA
					*p = 0x1F69;	//	Ὡ	0xe1 0xbd 0xa9	GREEK CAPITAL LETTER OMEGA WITH DASIA
					break;
				case 0x1F62:		//	ὢ	0xe1 0xbd 0xa2	GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA
					*p = 0x1F6A;	//	Ὢ	0xe1 0xbd 0xaa	GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA
					break;
				case 0x1F63:		//	ὣ	0xe1 0xbd 0xa3	GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA
					*p = 0x1F6B;	//	Ὣ	0xe1 0xbd 0xab	GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA
					break;
				case 0x1F64:		//	ὤ	0xe1 0xbd 0xa4	GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA
					*p = 0x1F6C;	//	Ὤ	0xe1 0xbd 0xac	GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA
					break;
				case 0x1F65:		//	ὥ	0xe1 0xbd 0xa5	GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA
					*p = 0x1F6D;	//	Ὥ	0xe1 0xbd 0xad	GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA
					break;
				case 0x1F66:		//	ὦ	0xe1 0xbd 0xa6	GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI
					*p = 0x1F6E;	//	Ὦ	0xe1 0xbd 0xae	GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F67:		//	ὧ	0xe1 0xbd 0xa7	GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI
					*p = 0x1F6F;	//	Ὧ	0xe1 0xbd 0xaf	GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F70:		//	ὰ	0xe1 0xbd 0xb0	GREEK SMALL LETTER ALPHA WITH VARIA
					*p = 0x1FBA;	//	Ὰ	0xe1 0xbe 0xba	GREEK CAPITAL LETTER ALPHA WITH VARIA
					break;
				case 0x1F71:		//	ά	0xe1 0xbd 0xb1	GREEK SMALL LETTER ALPHA WITH OXIA
					*p = 0x1FBB;	//	Ά	0xe1 0xbe 0xbb	GREEK CAPITAL LETTER ALPHA WITH OXIA
					break;
				case 0x1F72:		//	ὲ	0xe1 0xbd 0xb2	GREEK SMALL LETTER EPSILON WITH VARIA
					*p = 0x1FC8;	//	Ὲ	0xe1 0xbf 0x88	GREEK CAPITAL LETTER EPSILON WITH VARIA
					break;
				case 0x1F73:		//	έ	0xe1 0xbd 0xb3	GREEK SMALL LETTER EPSILON WITH OXIA
					*p = 0x1FC9;	//	Έ	0xe1 0xbf 0x89	GREEK CAPITAL LETTER EPSILON WITH OXIA
					break;
				case 0x1F74:		//	ὴ	0xe1 0xbd 0xb4	GREEK SMALL LETTER ETA WITH VARIA
					*p = 0x1FCA;	//	Ὴ	0xe1 0xbf 0x8a	GREEK CAPITAL LETTER ETA WITH VARIA
					break;
				case 0x1F75:		//	ή	0xe1 0xbd 0xb5	GREEK SMALL LETTER ETA WITH OXIA
					*p = 0x1FCB;	//	Ή	0xe1 0xbf 0x8b	GREEK CAPITAL LETTER ETA WITH OXIA
					break;
				case 0x1F76:		//	ὶ	0xe1 0xbd 0xb6	GREEK SMALL LETTER IOTA WITH VARIA
					*p = 0x1FDA;	//	Ὶ	0xe1 0xbf 0x9a	GREEK CAPITAL LETTER IOTA WITH VARIA
					break;
				case 0x1F77:		//	ί	0xe1 0xbd 0xb7	GREEK SMALL LETTER IOTA WITH OXIA
					*p = 0x1FDB;	//	Ί	0xe1 0xbf 0x9b	GREEK CAPITAL LETTER IOTA WITH OXIA
					break;
				case 0x1F78:		//	ὸ	0xe1 0xbd 0xb8	GREEK SMALL LETTER OMICRON WITH VARIA
					*p = 0x1FF8;	//	Ὸ	0xe1 0xbf 0xb8	GREEK CAPITAL LETTER OMICRON WITH VARIA
					break;
				case 0x1F79:		//	ό	0xe1 0xbd 0xb9	GREEK SMALL LETTER OMICRON WITH OXIA
					*p = 0x1FF9;	//	Ό	0xe1 0xbf 0xb9	GREEK CAPITAL LETTER OMICRON WITH OXIA
					break;
				case 0x1F7A:		//	ὺ	0xe1 0xbd 0xba	GREEK SMALL LETTER UPSILON WITH VARIA
					*p = 0x1FEA;	//	Ὺ	0xe1 0xbf 0xaa	GREEK CAPITAL LETTER UPSILON WITH VARIA
					break;
				case 0x1F7B:		//	ύ	0xe1 0xbd 0xbb	GREEK SMALL LETTER UPSILON WITH OXIA
					*p = 0x1FEB;	//	Ύ	0xe1 0xbf 0xab	GREEK CAPITAL LETTER UPSILON WITH OXIA
					break;
				case 0x1F7C:		//	ὼ	0xe1 0xbd 0xbc	GREEK SMALL LETTER OMEGA WITH VARIA
					*p = 0x1FFA;	//	Ὼ	0xe1 0xbf 0xba	GREEK CAPITAL LETTER OMEGA WITH VARIA
					break;
				case 0x1F7D:		//	ώ	0xe1 0xbd 0xbd	GREEK SMALL LETTER OMEGA WITH OXIA
					*p = 0x1FFB;	//	Ώ	0xe1 0xbf 0xbb	GREEK CAPITAL LETTER OMEGA WITH OXIA
					break;
				case 0x1F80:		//	ᾀ	0xe1 0xbe 0x80	GREEK SMALL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI
					*p = 0x1F88;	//	ᾈ	0xe1 0xbe 0x88	GREEK CAPITAL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI
					break;
				case 0x1F81:		//	ᾁ	0xe1 0xbe 0x81	GREEK SMALL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI
					*p = 0x1F89;	//	ᾉ	0xe1 0xbe 0x89	GREEK CAPITAL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI
					break;
				case 0x1F82:		//	ᾂ	0xe1 0xbe 0x82	GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F8A;	//	ᾊ	0xe1 0xbe 0x8a	GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F83:		//	ᾃ	0xe1 0xbe 0x83	GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F8B;	//	ᾋ	0xe1 0xbe 0x8b	GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F84:		//	ᾄ	0xe1 0xbe 0x84	GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F8C;	//	ᾌ	0xe1 0xbe 0x8c	GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F85:		//	ᾅ	0xe1 0xbe 0x85	GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F8D;	//	ᾍ	0xe1 0xbe 0x8d	GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F86:		//	ᾆ	0xe1 0xbe 0x86	GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F8E;	//	ᾎ	0xe1 0xbe 0x8e	GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1F87:		//	ᾇ	0xe1 0xbe 0x87	GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F8F;	//	ᾏ	0xe1 0xbe 0x8f	GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1F90:		//	ᾐ	0xe1 0xbe 0x90	GREEK SMALL LETTER ETA WITH PSILI AND PROSGEGRAMMENI
					*p = 0x1F98;	//	ᾘ	0xe1 0xbe 0x98	GREEK CAPITAL LETTER ETA WITH PSILI AND PROSGEGRAMMENI
					break;
				case 0x1F91:		//	ᾑ	0xe1 0xbe 0x91	GREEK SMALL LETTER ETA WITH DASIA AND PROSGEGRAMMENI
					*p = 0x1F99;	//	ᾙ	0xe1 0xbe 0x99	GREEK CAPITAL LETTER ETA WITH DASIA AND PROSGEGRAMMENI
					break;
				case 0x1F92:		//	ᾒ	0xe1 0xbe 0x92	GREEK SMALL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F9A;	//	ᾚ	0xe1 0xbe 0x9a	GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F93:		//	ᾓ	0xe1 0xbe 0x93	GREEK SMALL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F9B;	//	ᾛ	0xe1 0xbe 0x9b	GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F94:		//	ᾔ	0xe1 0xbe 0x94	GREEK SMALL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F9C;	//	ᾜ	0xe1 0xbe 0x9c	GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F95:		//	ᾕ	0xe1 0xbe 0x95	GREEK SMALL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F9D;	//	ᾝ	0xe1 0xbe 0x9d	GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F96:		//	ᾖ	0xe1 0xbe 0x96	GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F9E;	//	ᾞ	0xe1 0xbe 0x9e	GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1F97:		//	ᾗ	0xe1 0xbe 0x97	GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F9F;	//	ᾟ	0xe1 0xbe 0x9f	GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1FA0:		//	ᾠ	0xe1 0xbe 0xa0	GREEK SMALL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI
					*p = 0x1FA8;	//	ᾨ	0xe1 0xbe 0xa8	GREEK CAPITAL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI
					break;
				case 0x1FA1:		//	ᾡ	0xe1 0xbe 0xa1	GREEK SMALL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI
					*p = 0x1FA9;	//	ᾩ	0xe1 0xbe 0xa9	GREEK CAPITAL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI
					break;
				case 0x1FA2:		//	ᾢ	0xe1 0xbe 0xa2	GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					*p = 0x1FAA;	//	ᾪ	0xe1 0xbe 0xaa	GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1FA3:		//	ᾣ	0xe1 0xbe 0xa3	GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					*p = 0x1FAB;	//	ᾫ	0xe1 0xbe 0xab	GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1FA4:		//	ᾤ	0xe1 0xbe 0xa4	GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					*p = 0x1FAC;	//	ᾬ	0xe1 0xbe 0xac	GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1FA5:		//	ᾥ	0xe1 0xbe 0xa5	GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					*p = 0x1FAD;	//	ᾭ	0xe1 0xbe 0xad	GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1FA6:		//	ᾦ	0xe1 0xbe 0xa6	GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1FAE;	//	ᾮ	0xe1 0xbe 0xae	GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1FA7:		//	ᾧ	0xe1 0xbe 0xa7	GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1FAF;	//	ᾯ	0xe1 0xbe 0xaf	GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1FB0:		//	ᾰ	0xe1 0xbe 0xb0	GREEK SMALL LETTER ALPHA WITH VRACHY
					*p = 0x1FB8;	//	Ᾰ	0xe1 0xbe 0xb8	GREEK CAPITAL LETTER ALPHA WITH VRACHY
					break;
				case 0x1FB1:		//	ᾱ	0xe1 0xbe 0xb1	GREEK SMALL LETTER ALPHA WITH MACRON
					*p = 0x1FB9;	//	Ᾱ	0xe1 0xbe 0xb9	GREEK CAPITAL LETTER ALPHA WITH MACRON
					break;
				case 0x1FB3:		//	ᾳ	0xe1 0xbe 0xb3	GREEK SMALL LETTER ALPHA WITH PROSGEGRAMMENI
					*p = 0x1FBC;	//	ᾼ	0xe1 0xbe 0xbc	GREEK CAPITAL LETTER ALPHA WITH PROSGEGRAMMENI
					break;
				case 0x1FC3:		//	ῃ	0xe1 0xbf 0x83	GREEK SMALL LETTER ETA WITH PROSGEGRAMMENI
					*p = 0x1FCC;	//	ῌ	0xe1 0xbf 0x8c	GREEK CAPITAL LETTER ETA WITH PROSGEGRAMMENI
					break;
				case 0x1FD0:		//	ῐ	0xe1 0xbf 0x90	GREEK SMALL LETTER IOTA WITH VRACHY
					*p = 0x1FD8;	//	Ῐ	0xe1 0xbf 0x98	GREEK CAPITAL LETTER IOTA WITH VRACHY
					break;
				case 0x1FD1:		//	ῑ	0xe1 0xbf 0x91	GREEK SMALL LETTER IOTA WITH MACRON
					*p = 0x1FD9;	//	Ῑ	0xe1 0xbf 0x99	GREEK CAPITAL LETTER IOTA WITH MACRON
					break;
				case 0x1FE0:		//	ῠ	0xe1 0xbf 0xa0	GREEK SMALL LETTER UPSILON WITH VRACHY
					*p = 0x1FE8;	//	Ῠ	0xe1 0xbf 0xa8	GREEK CAPITAL LETTER UPSILON WITH VRACHY
					break;
				case 0x1FE1:		//	ῡ	0xe1 0xbf 0xa1	GREEK SMALL LETTER UPSILON WITH MACRON
					*p = 0x1FE9;	//	Ῡ	0xe1 0xbf 0xa9	GREEK CAPITAL LETTER UPSILON WITH MACRON
					break;
				case 0x1FE5:		//	ῥ	0xe1 0xbf 0xa5	GREEK SMALL LETTER RHO WITH DASIA
					*p = 0x1FEC;	//	Ῥ	0xe1 0xbf 0xac	GREEK CAPITAL LETTER RHO WITH DASIA
					break;
				case 0x1FF3:		//	ῳ	0xe1 0xbf 0xb3	GREEK SMALL LETTER OMEGA WITH PROSGEGRAMMENI
					*p = 0x1FFC;	//	ῼ	0xe1 0xbf 0xbc	GREEK CAPITAL LETTER OMEGA WITH PROSGEGRAMMENI
					break;
				case 0x2C30:		//	ⰰ	0xe2 0xb0 0xb0	GLAGOLITIC SMALL LETTER AZU
					*p = 0x2C00;	//	Ⰰ	0xe2 0xb0 0x80	GLAGOLITIC CAPITAL LETTER AZU
					break;
				case 0x2C31:		//	ⰱ	0xe2 0xb0 0xb1	GLAGOLITIC SMALL LETTER BUKY
					*p = 0x2C01;	//	Ⰱ	0xe2 0xb0 0x81	GLAGOLITIC CAPITAL LETTER BUKY
					break;
				case 0x2C32:		//	ⰲ	0xe2 0xb0 0xb2	GLAGOLITIC SMALL LETTER VEDE
					*p = 0x2C02;	//	Ⰲ	0xe2 0xb0 0x82	GLAGOLITIC CAPITAL LETTER VEDE
					break;
				case 0x2C33:		//	ⰳ	0xe2 0xb0 0xb3	GLAGOLITIC SMALL LETTER GLAGOLI
					*p = 0x2C03;	//	Ⰳ	0xe2 0xb0 0x83	GLAGOLITIC CAPITAL LETTER GLAGOLI
					break;
				case 0x2C34:		//	ⰴ	0xe2 0xb0 0xb4	GLAGOLITIC SMALL LETTER DOBRO
					*p = 0x2C04;	//	Ⰴ	0xe2 0xb0 0x84	GLAGOLITIC CAPITAL LETTER DOBRO
					break;
				case 0x2C35:		//	ⰵ	0xe2 0xb0 0xb5	GLAGOLITIC SMALL LETTER YESTU
					*p = 0x2C05;	//	Ⰵ	0xe2 0xb0 0x85	GLAGOLITIC CAPITAL LETTER YESTU
					break;
				case 0x2C36:		//	ⰶ	0xe2 0xb0 0xb6	GLAGOLITIC SMALL LETTER ZHIVETE
					*p = 0x2C06;	//	Ⰶ	0xe2 0xb0 0x86	GLAGOLITIC CAPITAL LETTER ZHIVETE
					break;
				case 0x2C37:		//	ⰷ	0xe2 0xb0 0xb7	GLAGOLITIC SMALL LETTER DZELO
					*p = 0x2C07;	//	Ⰷ	0xe2 0xb0 0x87	GLAGOLITIC CAPITAL LETTER DZELO
					break;
				case 0x2C38:		//	ⰸ	0xe2 0xb0 0xb8	GLAGOLITIC SMALL LETTER ZEMLJA
					*p = 0x2C08;	//	Ⰸ	0xe2 0xb0 0x88	GLAGOLITIC CAPITAL LETTER ZEMLJA
					break;
				case 0x2C39:		//	ⰹ	0xe2 0xb0 0xb9	GLAGOLITIC SMALL LETTER IZHE
					*p = 0x2C09;	//	Ⰹ	0xe2 0xb0 0x89	GLAGOLITIC CAPITAL LETTER IZHE
					break;
				case 0x2C3A:		//	ⰺ	0xe2 0xb0 0xba	GLAGOLITIC SMALL LETTER INITIAL IZHE
					*p = 0x2C0A;	//	Ⰺ	0xe2 0xb0 0x8a	GLAGOLITIC CAPITAL LETTER INITIAL IZHE
					break;
				case 0x2C3B:		//	ⰻ	0xe2 0xb0 0xbb	GLAGOLITIC SMALL LETTER I
					*p = 0x2C0B;	//	Ⰻ	0xe2 0xb0 0x8b	GLAGOLITIC CAPITAL LETTER I
					break;
				case 0x2C3C:		//	ⰼ	0xe2 0xb0 0xbc	GLAGOLITIC SMALL LETTER DJERVI
					*p = 0x2C0C;	//	Ⰼ	0xe2 0xb0 0x8c	GLAGOLITIC CAPITAL LETTER DJERVI
					break;
				case 0x2C3D:		//	ⰽ	0xe2 0xb0 0xbd	GLAGOLITIC SMALL LETTER KAKO
					*p = 0x2C0D;	//	Ⰽ	0xe2 0xb0 0x8d	GLAGOLITIC CAPITAL LETTER KAKO
					break;
				case 0x2C3E:		//	ⰾ	0xe2 0xb0 0xbe	GLAGOLITIC SMALL LETTER LJUDIJE
					*p = 0x2C0E;	//	Ⰾ	0xe2 0xb0 0x8e	GLAGOLITIC CAPITAL LETTER LJUDIJE
					break;
				case 0x2C3F:		//	ⰿ	0xe2 0xb0 0xbf	GLAGOLITIC SMALL LETTER MYSLITE
					*p = 0x2C0F;	//	Ⰿ	0xe2 0xb0 0x8f	GLAGOLITIC CAPITAL LETTER MYSLITE
					break;
				case 0x2C40:		//	ⱀ	0xe2 0xb1 0x80	GLAGOLITIC SMALL LETTER NASHI
					*p = 0x2C10;	//	Ⱀ	0xe2 0xb0 0x90	GLAGOLITIC CAPITAL LETTER NASHI
					break;
				case 0x2C41:		//	ⱁ	0xe2 0xb1 0x81	GLAGOLITIC SMALL LETTER ONU
					*p = 0x2C11;	//	Ⱁ	0xe2 0xb0 0x91	GLAGOLITIC CAPITAL LETTER ONU
					break;
				case 0x2C42:		//	ⱂ	0xe2 0xb1 0x82	GLAGOLITIC SMALL LETTER POKOJI
					*p = 0x2C12;	//	Ⱂ	0xe2 0xb0 0x92	GLAGOLITIC CAPITAL LETTER POKOJI
					break;
				case 0x2C43:		//	ⱃ	0xe2 0xb1 0x83	GLAGOLITIC SMALL LETTER RITSI
					*p = 0x2C13;	//	Ⱃ	0xe2 0xb0 0x93	GLAGOLITIC CAPITAL LETTER RITSI
					break;
				case 0x2C44:		//	ⱄ	0xe2 0xb1 0x84	GLAGOLITIC SMALL LETTER SLOVO
					*p = 0x2C14;	//	Ⱄ	0xe2 0xb0 0x94	GLAGOLITIC CAPITAL LETTER SLOVO
					break;
				case 0x2C45:		//	ⱅ	0xe2 0xb1 0x85	GLAGOLITIC SMALL LETTER TVRIDO
					*p = 0x2C15;	//	Ⱅ	0xe2 0xb0 0x95	GLAGOLITIC CAPITAL LETTER TVRIDO
					break;
				case 0x2C46:		//	ⱆ	0xe2 0xb1 0x86	GLAGOLITIC SMALL LETTER UKU
					*p = 0x2C16;	//	Ⱆ	0xe2 0xb0 0x96	GLAGOLITIC CAPITAL LETTER UKU
					break;
				case 0x2C47:		//	ⱇ	0xe2 0xb1 0x87	GLAGOLITIC SMALL LETTER FRITU
					*p = 0x2C17;	//	Ⱇ	0xe2 0xb0 0x97	GLAGOLITIC CAPITAL LETTER FRITU
					break;
				case 0x2C48:		//	ⱈ	0xe2 0xb1 0x88	GLAGOLITIC SMALL LETTER HERU
					*p = 0x2C18;	//	Ⱈ	0xe2 0xb0 0x98	GLAGOLITIC CAPITAL LETTER HERU
					break;
				case 0x2C49:		//	ⱉ	0xe2 0xb1 0x89	GLAGOLITIC SMALL LETTER OTU
					*p = 0x2C19;	//	Ⱉ	0xe2 0xb0 0x99	GLAGOLITIC CAPITAL LETTER OTU
					break;
				case 0x2C4A:		//	ⱊ	0xe2 0xb1 0x8a	GLAGOLITIC SMALL LETTER PE
					*p = 0x2C1A;	//	Ⱊ	0xe2 0xb0 0x9a	GLAGOLITIC CAPITAL LETTER PE
					break;
				case 0x2C4B:		//	ⱋ	0xe2 0xb1 0x8b	GLAGOLITIC SMALL LETTER SHTA
					*p = 0x2C1B;	//	Ⱋ	0xe2 0xb0 0x9b	GLAGOLITIC CAPITAL LETTER SHTA
					break;
				case 0x2C4C:		//	ⱌ	0xe2 0xb1 0x8c	GLAGOLITIC SMALL LETTER TSI
					*p = 0x2C1C;	//	Ⱌ	0xe2 0xb0 0x9c	GLAGOLITIC CAPITAL LETTER TSI
					break;
				case 0x2C4D:		//	ⱍ	0xe2 0xb1 0x8d	GLAGOLITIC SMALL LETTER CHRIVI
					*p = 0x2C1D;	//	Ⱍ	0xe2 0xb0 0x9d	GLAGOLITIC CAPITAL LETTER CHRIVI
					break;
				case 0x2C4E:		//	ⱎ	0xe2 0xb1 0x8e	GLAGOLITIC SMALL LETTER SHA
					*p = 0x2C1E;	//	Ⱎ	0xe2 0xb0 0x9e	GLAGOLITIC CAPITAL LETTER SHA
					break;
				case 0x2C4F:		//	ⱏ	0xe2 0xb1 0x8f	GLAGOLITIC SMALL LETTER YERU
					*p = 0x2C1F;	//	Ⱏ	0xe2 0xb0 0x9f	GLAGOLITIC CAPITAL LETTER YERU
					break;
				case 0x2C50:		//	ⱐ	0xe2 0xb1 0x90	GLAGOLITIC SMALL LETTER YERI
					*p = 0x2C20;	//	Ⱐ	0xe2 0xb0 0xa0	GLAGOLITIC CAPITAL LETTER YERI
					break;
				case 0x2C51:		//	ⱑ	0xe2 0xb1 0x91	GLAGOLITIC SMALL LETTER YATI
					*p = 0x2C21;	//	Ⱑ	0xe2 0xb0 0xa1	GLAGOLITIC CAPITAL LETTER YATI
					break;
				case 0x2C52:		//	ⱒ	0xe2 0xb1 0x92	GLAGOLITIC SMALL LETTER SPIDERY HA
					*p = 0x2C22;	//	Ⱒ	0xe2 0xb0 0xa2	GLAGOLITIC CAPITAL LETTER SPIDERY HA
					break;
				case 0x2C53:		//	ⱓ	0xe2 0xb1 0x93	GLAGOLITIC SMALL LETTER YU
					*p = 0x2C23;	//	Ⱓ	0xe2 0xb0 0xa3	GLAGOLITIC CAPITAL LETTER YU
					break;
				case 0x2C54:		//	ⱔ	0xe2 0xb1 0x94	GLAGOLITIC SMALL LETTER SMALL YUS
					*p = 0x2C24;	//	Ⱔ	0xe2 0xb0 0xa4	GLAGOLITIC CAPITAL LETTER SMALL YUS
					break;
				case 0x2C55:		//	ⱕ	0xe2 0xb1 0x95	GLAGOLITIC SMALL LETTER SMALL YUS WITH TAIL
					*p = 0x2C25;	//	Ⱕ	0xe2 0xb0 0xa5	GLAGOLITIC CAPITAL LETTER SMALL YUS WITH TAIL
					break;
				case 0x2C56:		//	ⱖ	0xe2 0xb1 0x96	GLAGOLITIC SMALL LETTER YO
					*p = 0x2C26;	//	Ⱖ	0xe2 0xb0 0xa6	GLAGOLITIC CAPITAL LETTER YO
					break;
				case 0x2C57:		//	ⱗ	0xe2 0xb1 0x97	GLAGOLITIC SMALL LETTER IOTATED SMALL YUS
					*p = 0x2C27;	//	Ⱗ	0xe2 0xb0 0xa7	GLAGOLITIC CAPITAL LETTER IOTATED SMALL YUS
					break;
				case 0x2C58:		//	ⱘ	0xe2 0xb1 0x98	GLAGOLITIC SMALL LETTER BIG YUS
					*p = 0x2C28;	//	Ⱘ	0xe2 0xb0 0xa8	GLAGOLITIC CAPITAL LETTER BIG YUS
					break;
				case 0x2C59:		//	ⱙ	0xe2 0xb1 0x99	GLAGOLITIC SMALL LETTER IOTATED BIG YUS
					*p = 0x2C29;	//	Ⱙ	0xe2 0xb0 0xa9	GLAGOLITIC CAPITAL LETTER IOTATED BIG YUS
					break;
				case 0x2C5A:		//	ⱚ	0xe2 0xb1 0x9a	GLAGOLITIC SMALL LETTER FITA
					*p = 0x2C2A;	//	Ⱚ	0xe2 0xb0 0xaa	GLAGOLITIC CAPITAL LETTER FITA
					break;
				case 0x2C5B:		//	ⱛ	0xe2 0xb1 0x9b	GLAGOLITIC SMALL LETTER IZHITSA
					*p = 0x2C2B;	//	Ⱛ	0xe2 0xb0 0xab	GLAGOLITIC CAPITAL LETTER IZHITSA
					break;
				case 0x2C5C:		//	ⱜ	0xe2 0xb1 0x9c	GLAGOLITIC SMALL LETTER SHTAPIC
					*p = 0x2C2C;	//	Ⱜ	0xe2 0xb0 0xac	GLAGOLITIC CAPITAL LETTER SHTAPIC
					break;
				case 0x2C5D:		//	ⱝ	0xe2 0xb1 0x9d	GLAGOLITIC SMALL LETTER TROKUTASTI A
					*p = 0x2C2D;	//	Ⱝ	0xe2 0xb0 0xad	GLAGOLITIC CAPITAL LETTER TROKUTASTI A
					break;
				case 0x2C5E:		//	ⱞ	0xe2 0xb1 0x9e	GLAGOLITIC SMALL LETTER LATINATE MYSLITE
					*p = 0x2C2E;	//	Ⱞ	0xe2 0xb0 0xae	GLAGOLITIC CAPITAL LETTER LATINATE MYSLITE
					break;
				case 0x2C61:		//	ⱡ	0xe2 0xb1 0xa1	LATIN SMALL LETTER L WITH DOUBLE BAR
					*p = 0x2C60;	//	Ⱡ	0xe2 0xb1 0xa0	LATIN CAPITAL LETTER L WITH DOUBLE BAR
					break;
				case 0x2C65:		//	ⱥ	0xe2 0xb1 0xa5	LATIN SMALL LETTER A WITH STROKE
					*p = 0x023A;	//	Ⱥ	0xc8 0xba	LATIN CAPITAL LETTER A WITH STROKE
					break;
				case 0x2C66:		//	ⱦ	0xe2 0xb1 0xa6	LATIN SMALL LETTER T WITH DIAGONAL STROKE
					*p = 0x023E;	//	Ⱦ	0xc8 0xbe	LATIN CAPITAL LETTER T WITH DIAGONAL STROKE
					break;
				case 0x2C68:		//	ⱨ	0xe2 0xb1 0xa8	LATIN SMALL LETTER H WITH DESCENDER
					*p = 0x2C67;	//	Ⱨ	0xe2 0xb1 0xa7	LATIN CAPITAL LETTER H WITH DESCENDER
					break;
				case 0x2C6A:		//	ⱪ	0xe2 0xb1 0xaa	LATIN SMALL LETTER K WITH DESCENDER
					*p = 0x2C69;	//	Ⱪ	0xe2 0xb1 0xa9	LATIN CAPITAL LETTER K WITH DESCENDER
					break;
				case 0x2C6C:		//	ⱬ	0xe2 0xb1 0xac	LATIN SMALL LETTER Z WITH DESCENDER
					*p = 0x2C6B;	//	Ⱬ	0xe2 0xb1 0xab	LATIN CAPITAL LETTER Z WITH DESCENDER
					break;
				case 0x2C73:		//	ⱳ	0xe2 0xb1 0xb3	LATIN SMALL LETTER W WITH HOOK
					*p = 0x2C72;	//	Ⱳ	0xe2 0xb1 0xb2	LATIN CAPITAL LETTER W WITH HOOK
					break;
				case 0x2C76:		//	ⱶ	0xe2 0xb1 0xb6	LATIN SMALL LETTER HALF H
					*p = 0x2C75;	//	Ⱶ	0xe2 0xb1 0xb5	LATIN CAPITAL LETTER HALF H
					break;
				case 0x2C81:		//	ⲁ	0xe2 0xb2 0x81	COPTIC SMALL LETTER ALFA
					*p = 0x2C80;	//	Ⲁ	0xe2 0xb2 0x80	COPTIC CAPITAL LETTER ALFA
					break;
				case 0x2C83:		//	ⲃ	0xe2 0xb2 0x83	COPTIC SMALL LETTER VIDA
					*p = 0x2C82;	//	Ⲃ	0xe2 0xb2 0x82	COPTIC CAPITAL LETTER VIDA
					break;
				case 0x2C85:		//	ⲅ	0xe2 0xb2 0x85	COPTIC SMALL LETTER GAMMA
					*p = 0x2C84;	//	Ⲅ	0xe2 0xb2 0x84	COPTIC CAPITAL LETTER GAMMA
					break;
				case 0x2C87:		//	ⲇ	0xe2 0xb2 0x87	COPTIC SMALL LETTER DALDA
					*p = 0x2C86;	//	Ⲇ	0xe2 0xb2 0x86	COPTIC CAPITAL LETTER DALDA
					break;
				case 0x2C89:		//	ⲉ	0xe2 0xb2 0x89	COPTIC SMALL LETTER EIE
					*p = 0x2C88;	//	Ⲉ	0xe2 0xb2 0x88	COPTIC CAPITAL LETTER EIE
					break;
				case 0x2C8B:		//	ⲋ	0xe2 0xb2 0x8b	COPTIC SMALL LETTER SOU
					*p = 0x2C8A;	//	Ⲋ	0xe2 0xb2 0x8a	COPTIC CAPITAL LETTER SOU
					break;
				case 0x2C8D:		//	ⲍ	0xe2 0xb2 0x8d	COPTIC SMALL LETTER ZATA
					*p = 0x2C8C;	//	Ⲍ	0xe2 0xb2 0x8c	COPTIC CAPITAL LETTER ZATA
					break;
				case 0x2C8F:		//	ⲏ	0xe2 0xb2 0x8f	COPTIC SMALL LETTER HATE
					*p = 0x2C8E;	//	Ⲏ	0xe2 0xb2 0x8e	COPTIC CAPITAL LETTER HATE
					break;
				case 0x2C91:		//	ⲑ	0xe2 0xb2 0x91	COPTIC SMALL LETTER THETHE
					*p = 0x2C90;	//	Ⲑ	0xe2 0xb2 0x90	COPTIC CAPITAL LETTER THETHE
					break;
				case 0x2C93:		//	ⲓ	0xe2 0xb2 0x93	COPTIC SMALL LETTER IAUDA
					*p = 0x2C92;	//	Ⲓ	0xe2 0xb2 0x92	COPTIC CAPITAL LETTER IAUDA
					break;
				case 0x2C95:		//	ⲕ	0xe2 0xb2 0x95	COPTIC SMALL LETTER KAPA
					*p = 0x2C94;	//	Ⲕ	0xe2 0xb2 0x94	COPTIC CAPITAL LETTER KAPA
					break;
				case 0x2C97:		//	ⲗ	0xe2 0xb2 0x97	COPTIC SMALL LETTER LAULA
					*p = 0x2C96;	//	Ⲗ	0xe2 0xb2 0x96	COPTIC CAPITAL LETTER LAULA
					break;
				case 0x2C99:		//	ⲙ	0xe2 0xb2 0x99	COPTIC SMALL LETTER MI
					*p = 0x2C98;	//	Ⲙ	0xe2 0xb2 0x98	COPTIC CAPITAL LETTER MI
					break;
				case 0x2C9B:		//	ⲛ	0xe2 0xb2 0x9b	COPTIC SMALL LETTER NI
					*p = 0x2C9A;	//	Ⲛ	0xe2 0xb2 0x9a	COPTIC CAPITAL LETTER NI
					break;
				case 0x2C9D:		//	ⲝ	0xe2 0xb2 0x9d	COPTIC SMALL LETTER KSI
					*p = 0x2C9C;	//	Ⲝ	0xe2 0xb2 0x9c	COPTIC CAPITAL LETTER KSI
					break;
				case 0x2C9F:		//	ⲟ	0xe2 0xb2 0x9f	COPTIC SMALL LETTER O
					*p = 0x2C9E;	//	Ⲟ	0xe2 0xb2 0x9e	COPTIC CAPITAL LETTER O
					break;
				case 0x2CA1:		//	ⲡ	0xe2 0xb2 0xa1	COPTIC SMALL LETTER PI
					*p = 0x2CA0;	//	Ⲡ	0xe2 0xb2 0xa0	COPTIC CAPITAL LETTER PI
					break;
				case 0x2CA3:		//	ⲣ	0xe2 0xb2 0xa3	COPTIC SMALL LETTER RO
					*p = 0x2CA2;	//	Ⲣ	0xe2 0xb2 0xa2	COPTIC CAPITAL LETTER RO
					break;
				case 0x2CA5:		//	ⲥ	0xe2 0xb2 0xa5	COPTIC SMALL LETTER SIMA
					*p = 0x2CA4;	//	Ⲥ	0xe2 0xb2 0xa4	COPTIC CAPITAL LETTER SIMA
					break;
				case 0x2CA7:		//	ⲧ	0xe2 0xb2 0xa7	COPTIC SMALL LETTER TAU
					*p = 0x2CA6;	//	Ⲧ	0xe2 0xb2 0xa6	COPTIC CAPITAL LETTER TAU
					break;
				case 0x2CA9:		//	ⲩ	0xe2 0xb2 0xa9	COPTIC SMALL LETTER UA
					*p = 0x2CA8;	//	Ⲩ	0xe2 0xb2 0xa8	COPTIC CAPITAL LETTER UA
					break;
				case 0x2CAB:		//	ⲫ	0xe2 0xb2 0xab	COPTIC SMALL LETTER FI
					*p = 0x2CAA;	//	Ⲫ	0xe2 0xb2 0xaa	COPTIC CAPITAL LETTER FI
					break;
				case 0x2CAD:		//	ⲭ	0xe2 0xb2 0xad	COPTIC SMALL LETTER KHI
					*p = 0x2CAC;	//	Ⲭ	0xe2 0xb2 0xac	COPTIC CAPITAL LETTER KHI
					break;
				case 0x2CAF:		//	ⲯ	0xe2 0xb2 0xaf	COPTIC SMALL LETTER PSI
					*p = 0x2CAE;	//	Ⲯ	0xe2 0xb2 0xae	COPTIC CAPITAL LETTER PSI
					break;
				case 0x2CB1:		//	ⲱ	0xe2 0xb2 0xb1	COPTIC SMALL LETTER OOU
					*p = 0x2CB0;	//	Ⲱ	0xe2 0xb2 0xb0	COPTIC CAPITAL LETTER OOU
					break;
				case 0x2CB3:		//	ⲳ	0xe2 0xb2 0xb3	COPTIC SMALL LETTER DIALECT-P ALEF
					*p = 0x2CB2;	//	Ⲳ	0xe2 0xb2 0xb2	COPTIC CAPITAL LETTER DIALECT-P ALEF
					break;
				case 0x2CB5:		//	ⲵ	0xe2 0xb2 0xb5	COPTIC SMALL LETTER OLD COPTIC AIN
					*p = 0x2CB4;	//	Ⲵ	0xe2 0xb2 0xb4	COPTIC CAPITAL LETTER OLD COPTIC AIN
					break;
				case 0x2CB7:		//	ⲷ	0xe2 0xb2 0xb7	COPTIC SMALL LETTER CRYPTOGRAMMIC EIE
					*p = 0x2CB6;	//	Ⲷ	0xe2 0xb2 0xb6	COPTIC CAPITAL LETTER CRYPTOGRAMMIC EIE
					break;
				case 0x2CB9:		//	ⲹ	0xe2 0xb2 0xb9	COPTIC SMALL LETTER DIALECT-P KAPA
					*p = 0x2CB8;	//	Ⲹ	0xe2 0xb2 0xb8	COPTIC CAPITAL LETTER DIALECT-P KAPA
					break;
				case 0x2CBB:		//	ⲻ	0xe2 0xb2 0xbb	COPTIC SMALL LETTER DIALECT-P NI
					*p = 0x2CBA;	//	Ⲻ	0xe2 0xb2 0xba	COPTIC CAPITAL LETTER DIALECT-P NI
					break;
				case 0x2CBD:		//	ⲽ	0xe2 0xb2 0xbd	COPTIC SMALL LETTER CRYPTOGRAMMIC NI
					*p = 0x2CBC;	//	Ⲽ	0xe2 0xb2 0xbc	COPTIC CAPITAL LETTER CRYPTOGRAMMIC NI
					break;
				case 0x2CBF:		//	ⲿ	0xe2 0xb2 0xbf	COPTIC SMALL LETTER OLD COPTIC OOU
					*p = 0x2CBE;	//	Ⲿ	0xe2 0xb2 0xbe	COPTIC CAPITAL LETTER OLD COPTIC OOU
					break;
				case 0x2CC1:		//	ⳁ	0xe2 0xb3 0x81	COPTIC SMALL LETTER SAMPI
					*p = 0x2CC0;	//	Ⳁ	0xe2 0xb3 0x80	COPTIC CAPITAL LETTER SAMPI
					break;
				case 0x2CC3:		//	ⳃ	0xe2 0xb3 0x83	COPTIC SMALL LETTER CROSSED SHEI
					*p = 0x2CC2;	//	Ⳃ	0xe2 0xb3 0x82	COPTIC CAPITAL LETTER CROSSED SHEI
					break;
				case 0x2CC5:		//	ⳅ	0xe2 0xb3 0x85	COPTIC SMALL LETTER OLD COPTIC SHEI
					*p = 0x2CC4;	//	Ⳅ	0xe2 0xb3 0x84	COPTIC CAPITAL LETTER OLD COPTIC SHEI
					break;
				case 0x2CC7:		//	ⳇ	0xe2 0xb3 0x87	COPTIC SMALL LETTER OLD COPTIC ESH
					*p = 0x2CC6;	//	Ⳇ	0xe2 0xb3 0x86	COPTIC CAPITAL LETTER OLD COPTIC ESH
					break;
				case 0x2CC9:		//	ⳉ	0xe2 0xb3 0x89	COPTIC SMALL LETTER AKHMIMIC KHEI
					*p = 0x2CC8;	//	Ⳉ	0xe2 0xb3 0x88	COPTIC CAPITAL LETTER AKHMIMIC KHEI
					break;
				case 0x2CCB:		//	ⳋ	0xe2 0xb3 0x8b	COPTIC SMALL LETTER DIALECT-P HORI
					*p = 0x2CCA;	//	Ⳋ	0xe2 0xb3 0x8a	COPTIC CAPITAL LETTER DIALECT-P HORI
					break;
				case 0x2CCD:		//	ⳍ	0xe2 0xb3 0x8d	COPTIC SMALL LETTER OLD COPTIC HORI
					*p = 0x2CCC;	//	Ⳍ	0xe2 0xb3 0x8c	COPTIC CAPITAL LETTER OLD COPTIC HORI
					break;
				case 0x2CCF:		//	ⳏ	0xe2 0xb3 0x8f	COPTIC SMALL LETTER OLD COPTIC HA
					*p = 0x2CCE;	//	Ⳏ	0xe2 0xb3 0x8e	COPTIC CAPITAL LETTER OLD COPTIC HA
					break;
				case 0x2CD1:		//	ⳑ	0xe2 0xb3 0x91	COPTIC SMALL LETTER L-SHAPED HA
					*p = 0x2CD0;	//	Ⳑ	0xe2 0xb3 0x90	COPTIC CAPITAL LETTER L-SHAPED HA
					break;
				case 0x2CD3:		//	ⳓ	0xe2 0xb3 0x93	COPTIC SMALL LETTER OLD COPTIC HEI
					*p = 0x2CD2;	//	Ⳓ	0xe2 0xb3 0x92	COPTIC CAPITAL LETTER OLD COPTIC HEI
					break;
				case 0x2CD5:		//	ⳕ	0xe2 0xb3 0x95	COPTIC SMALL LETTER OLD COPTIC HAT
					*p = 0x2CD4;	//	Ⳕ	0xe2 0xb3 0x94	COPTIC CAPITAL LETTER OLD COPTIC HAT
					break;
				case 0x2CD7:		//	ⳗ	0xe2 0xb3 0x97	COPTIC SMALL LETTER OLD COPTIC GANGIA
					*p = 0x2CD6;	//	Ⳗ	0xe2 0xb3 0x96	COPTIC CAPITAL LETTER OLD COPTIC GANGIA
					break;
				case 0x2CD9:		//	ⳙ	0xe2 0xb3 0x99	COPTIC SMALL LETTER OLD COPTIC DJA
					*p = 0x2CD8;	//	Ⳙ	0xe2 0xb3 0x98	COPTIC CAPITAL LETTER OLD COPTIC DJA
					break;
				case 0x2CDB:		//	ⳛ	0xe2 0xb3 0x9b	COPTIC SMALL LETTER OLD COPTIC SHIMA
					*p = 0x2CDA;	//	Ⳛ	0xe2 0xb3 0x9a	COPTIC CAPITAL LETTER OLD COPTIC SHIMA
					break;
				case 0x2CDD:		//	ⳝ	0xe2 0xb3 0x9d	COPTIC SMALL LETTER OLD NUBIAN SHIMA
					*p = 0x2CDC;	//	Ⳝ	0xe2 0xb3 0x9c	COPTIC CAPITAL LETTER OLD NUBIAN SHIMA
					break;
				case 0x2CDF:		//	ⳟ	0xe2 0xb3 0x9f	COPTIC SMALL LETTER OLD NUBIAN NGI
					*p = 0x2CDE;	//	Ⳟ	0xe2 0xb3 0x9e	COPTIC CAPITAL LETTER OLD NUBIAN NGI
					break;
				case 0x2CE1:		//	ⳡ	0xe2 0xb3 0xa1	COPTIC SMALL LETTER OLD NUBIAN NYI
					*p = 0x2CE0;	//	Ⳡ	0xe2 0xb3 0xa0	COPTIC CAPITAL LETTER OLD NUBIAN NYI
					break;
				case 0x2CE3:		//	ⳣ	0xe2 0xb3 0xa3	COPTIC SMALL LETTER OLD NUBIAN WAU
					*p = 0x2CE2;	//	Ⳣ	0xe2 0xb3 0xa2	COPTIC CAPITAL LETTER OLD NUBIAN WAU
					break;
				case 0x2CEC:		//	ⳬ	0xe2 0xb3 0xac	COPTIC SMALL LETTER CRYPTOGRAMMIC SHEI
					*p = 0x2CEB;	//	Ⳬ	0xe2 0xb3 0xab	COPTIC CAPITAL LETTER CRYPTOGRAMMIC SHEI
					break;
				case 0x2CEE:		//	ⳮ	0xe2 0xb3 0xae	COPTIC SMALL LETTER CRYPTOGRAMMIC GANGIA
					*p = 0x2CED;	//	Ⳮ	0xe2 0xb3 0xad	COPTIC CAPITAL LETTER CRYPTOGRAMMIC GANGIA
					break;
				case 0x2CF3:		//	ⳳ	0xe2 0xb3 0xb3	COPTIC SMALL LETTER BOHAIRIC KHEI
					*p = 0x2CF2;	//	Ⳳ	0xe2 0xb3 0xb2	COPTIC CAPITAL LETTER BOHAIRIC KHEI
					break;
				case 0x2D00:		//	ⴀ	0xe2 0xb4 0x80	GEORGIAN LETTER AN
					*p = 0x1C90;	//	Ა	0xe1 0xb2 0x90	GEORGIAN CAPITAL LETTER AN
					break;
				case 0x2D01:		//	ⴁ	0xe2 0xb4 0x81	GEORGIAN LETTER BAN
					*p = 0x1C91;	//	Ბ	0xe1 0xb2 0x91	GEORGIAN CAPITAL LETTER BAN
					break;
				case 0x2D02:		//	ⴂ	0xe2 0xb4 0x82	GEORGIAN LETTER GAN
					*p = 0x1C92;	//	Გ	0xe1 0xb2 0x92	GEORGIAN CAPITAL LETTER GAN
					break;
				case 0x2D03:		//	ⴃ	0xe2 0xb4 0x83	GEORGIAN LETTER DON
					*p = 0x1C93;	//	Დ	0xe1 0xb2 0x93	GEORGIAN CAPITAL LETTER DON
					break;
				case 0x2D04:		//	ⴄ	0xe2 0xb4 0x84	GEORGIAN LETTER EN
					*p = 0x1C94;	//	Ე	0xe1 0xb2 0x94	GEORGIAN CAPITAL LETTER EN
					break;
				case 0x2D05:		//	ⴅ	0xe2 0xb4 0x85	GEORGIAN LETTER VIN
					*p = 0x1C95;	//	Ვ	0xe1 0xb2 0x95	GEORGIAN CAPITAL LETTER VIN
					break;
				case 0x2D06:		//	ⴆ	0xe2 0xb4 0x86	GEORGIAN LETTER ZEN
					*p = 0x1C96;	//	Ზ	0xe1 0xb2 0x96	GEORGIAN CAPITAL LETTER ZEN
					break;
				case 0x2D07:		//	ⴇ	0xe2 0xb4 0x87	GEORGIAN LETTER TAN
					*p = 0x1C97;	//	Თ	0xe1 0xb2 0x97	GEORGIAN CAPITAL LETTER TAN
					break;
				case 0x2D08:		//	ⴈ	0xe2 0xb4 0x88	GEORGIAN LETTER IN
					*p = 0x1C98;	//	Ი	0xe1 0xb2 0x98	GEORGIAN CAPITAL LETTER IN
					break;
				case 0x2D09:		//	ⴉ	0xe2 0xb4 0x89	GEORGIAN LETTER KAN
					*p = 0x1C99;	//	Კ	0xe1 0xb2 0x99	GEORGIAN CAPITAL LETTER KAN
					break;
				case 0x2D0A:		//	ⴊ	0xe2 0xb4 0x8a	GEORGIAN LETTER LAS
					*p = 0x1C9A;	//	Ლ	0xe1 0xb2 0x9a	GEORGIAN CAPITAL LETTER LAS
					break;
				case 0x2D0B:		//	ⴋ	0xe2 0xb4 0x8b	GEORGIAN LETTER MAN
					*p = 0x1C9B;	//	Მ	0xe1 0xb2 0x9b	GEORGIAN CAPITAL LETTER MAN
					break;
				case 0x2D0C:		//	ⴌ	0xe2 0xb4 0x8c	GEORGIAN LETTER NAR
					*p = 0x1C9C;	//	Ნ	0xe1 0xb2 0x9c	GEORGIAN CAPITAL LETTER NAR
					break;
				case 0x2D0D:		//	ⴍ	0xe2 0xb4 0x8d	GEORGIAN LETTER ON
					*p = 0x1C9D;	//	Ო	0xe1 0xb2 0x9d	GEORGIAN CAPITAL LETTER ON
					break;
				case 0x2D0E:		//	ⴎ	0xe2 0xb4 0x8e	GEORGIAN LETTER PAR
					*p = 0x1C9E;	//	Პ	0xe1 0xb2 0x9e	GEORGIAN CAPITAL LETTER PAR
					break;
				case 0x2D0F:		//	ⴏ	0xe2 0xb4 0x8f	GEORGIAN LETTER ZHAR
					*p = 0x1C9F;	//	Ჟ	0xe1 0xb2 0x9f	GEORGIAN CAPITAL LETTER ZHAR
					break;
				case 0x2D10:		//	ⴐ	0xe2 0xb4 0x90	GEORGIAN LETTER RAE
					*p = 0x1CA0;	//	Რ	0xe1 0xb2 0xa0	GEORGIAN CAPITAL LETTER RAE
					break;
				case 0x2D11:		//	ⴑ	0xe2 0xb4 0x91	GEORGIAN LETTER SAN
					*p = 0x1CA1;	//	Ს	0xe1 0xb2 0xa1	GEORGIAN CAPITAL LETTER SAN
					break;
				case 0x2D12:		//	ⴒ	0xe2 0xb4 0x92	GEORGIAN LETTER TAR
					*p = 0x1CA2;	//	Ტ	0xe1 0xb2 0xa2	GEORGIAN CAPITAL LETTER TAR
					break;
				case 0x2D13:		//	ⴓ	0xe2 0xb4 0x93	GEORGIAN LETTER UN
					*p = 0x1CA3;	//	Უ	0xe1 0xb2 0xa3	GEORGIAN CAPITAL LETTER UN
					break;
				case 0x2D14:		//	ⴔ	0xe2 0xb4 0x94	GEORGIAN LETTER PHAR
					*p = 0x1CA4;	//	Ფ	0xe1 0xb2 0xa4	GEORGIAN CAPITAL LETTER PHAR
					break;
				case 0x2D15:		//	ⴕ	0xe2 0xb4 0x95	GEORGIAN LETTER KHAR
					*p = 0x1CA5;	//	Ქ	0xe1 0xb2 0xa5	GEORGIAN CAPITAL LETTER KHAR
					break;
				case 0x2D16:		//	ⴖ	0xe2 0xb4 0x96	GEORGIAN LETTER GHAN
					*p = 0x1CA6;	//	Ღ	0xe1 0xb2 0xa6	GEORGIAN CAPITAL LETTER GHAN
					break;
				case 0x2D17:		//	ⴗ	0xe2 0xb4 0x97	GEORGIAN LETTER QAR
					*p = 0x1CA7;	//	Ყ	0xe1 0xb2 0xa7	GEORGIAN CAPITAL LETTER QAR
					break;
				case 0x2D18:		//	ⴘ	0xe2 0xb4 0x98	GEORGIAN LETTER SHIN
					*p = 0x1CA8;	//	Შ	0xe1 0xb2 0xa8	GEORGIAN CAPITAL LETTER SHIN
					break;
				case 0x2D19:		//	ⴙ	0xe2 0xb4 0x99	GEORGIAN LETTER CHIN
					*p = 0x1CA9;	//	Ჩ	0xe1 0xb2 0xa9	GEORGIAN CAPITAL LETTER CHIN
					break;
				case 0x2D1A:		//	ⴚ	0xe2 0xb4 0x9a	GEORGIAN LETTER CAN
					*p = 0x1CAA;	//	Ც	0xe1 0xb2 0xaa	GEORGIAN CAPITAL LETTER CAN
					break;
				case 0x2D1B:		//	ⴛ	0xe2 0xb4 0x9b	GEORGIAN LETTER JIL
					*p = 0x1CAB;	//	Ძ	0xe1 0xb2 0xab	GEORGIAN CAPITAL LETTER JIL
					break;
				case 0x2D1C:		//	ⴜ	0xe2 0xb4 0x9c	GEORGIAN LETTER CIL
					*p = 0x1CAC;	//	Წ	0xe1 0xb2 0xac	GEORGIAN CAPITAL LETTER CIL
					break;
				case 0x2D1D:		//	ⴝ	0xe2 0xb4 0x9d	GEORGIAN LETTER CHAR
					*p = 0x1CAD;	//	Ჭ	0xe1 0xb2 0xad	GEORGIAN CAPITAL LETTER CHAR
					break;
				case 0x2D1E:		//	ⴞ	0xe2 0xb4 0x9e	GEORGIAN LETTER XAN
					*p = 0x1CAE;	//	Ხ	0xe1 0xb2 0xae	GEORGIAN CAPITAL LETTER XAN
					break;
				case 0x2D1F:		//	ⴟ	0xe2 0xb4 0x9f	GEORGIAN LETTER JHAN
					*p = 0x1CAF;	//	Ჯ	0xe1 0xb2 0xaf	GEORGIAN CAPITAL LETTER JHAN
					break;
				case 0x2D20:		//	ⴠ	0xe2 0xb4 0xa0	GEORGIAN LETTER HAE
					*p = 0x1CB0;	//	Ჰ	0xe1 0xb2 0xb0	GEORGIAN CAPITAL LETTER HAE
					break;
				case 0x2D21:		//	ⴡ	0xe2 0xb4 0xa1	GEORGIAN LETTER HE
					*p = 0x1CB1;	//	Ჱ	0xe1 0xb2 0xb1	GEORGIAN CAPITAL LETTER HE
					break;
				case 0x2D22:		//	ⴢ	0xe2 0xb4 0xa2	GEORGIAN LETTER HIE
					*p = 0x1CB2;	//	Ჲ	0xe1 0xb2 0xb2	GEORGIAN CAPITAL LETTER HIE
					break;
				case 0x2D23:		//	ⴣ	0xe2 0xb4 0xa3	GEORGIAN LETTER WE
					*p = 0x1CB3;	//	Ჳ	0xe1 0xb2 0xb3	GEORGIAN CAPITAL LETTER WE
					break;
				case 0x2D24:		//	ⴤ	0xe2 0xb4 0xa4	GEORGIAN LETTER HAR
					*p = 0x1CB4;	//	Ჴ	0xe1 0xb2 0xb4	GEORGIAN CAPITAL LETTER HAR
					break;
				case 0x2D25:		//	ⴥ	0xe2 0xb4 0xa5	GEORGIAN LETTER HOE
					*p = 0x1CB5;	//	Ჵ	0xe1 0xb2 0xb5	GEORGIAN CAPITAL LETTER HOE
					break;
				case 0x2D27:		//	ⴧ	0xe2 0xb4 0xa7	GEORGIAN LETTER YN
					*p = 0x1CB7;	//	Ჷ	0xe1 0xb2 0xb7	GEORGIAN CAPITAL LETTER YN
					break;
				case 0x2D2D:		//	ⴭ	0xe2 0xb4 0xad	GEORGIAN LETTER AEN
					*p = 0x1CBD;	//	Ჽ	0xe1 0xb2 0xbd	GEORGIAN CAPITAL LETTER AEN
					break;
				case 0xA641:		//	ꙁ	0xea 0x99 0x81	CYRILLIC SMALL LETTER ZEMLYA
					*p = 0xA640;	//	Ꙁ	0xea 0x99 0x80	CYRILLIC CAPITAL LETTER ZEMLYA
					break;
				case 0xA643:		//	ꙃ	0xea 0x99 0x83	CYRILLIC SMALL LETTER DZELO
					*p = 0xA642;	//	Ꙃ	0xea 0x99 0x82	CYRILLIC CAPITAL LETTER DZELO
					break;
				case 0xA645:		//	ꙅ	0xea 0x99 0x85	CYRILLIC SMALL LETTER REVERSED DZE
					*p = 0xA644;	//	Ꙅ	0xea 0x99 0x84	CYRILLIC CAPITAL LETTER REVERSED DZE
					break;
				case 0xA647:		//	ꙇ	0xea 0x99 0x87	CYRILLIC SMALL LETTER IOTA
					*p = 0xA646;	//	Ꙇ	0xea 0x99 0x86	CYRILLIC CAPITAL LETTER IOTA
					break;
				case 0xA649:		//	ꙉ	0xea 0x99 0x89	CYRILLIC SMALL LETTER DJERV
					*p = 0xA648;	//	Ꙉ	0xea 0x99 0x88	CYRILLIC CAPITAL LETTER DJERV
					break;
				case 0xA64B:		//	ꙋ	0xea 0x99 0x8b	CYRILLIC SMALL LETTER MONOGRAPH UK
					*p = 0xA64A;	//	Ꙋ	0xea 0x99 0x8a	CYRILLIC CAPITAL LETTER MONOGRAPH UK
					break;
				case 0xA64D:		//	ꙍ	0xea 0x99 0x8d	CYRILLIC SMALL LETTER BROAD OMEGA
					*p = 0xA64C;	//	Ꙍ	0xea 0x99 0x8c	CYRILLIC CAPITAL LETTER BROAD OMEGA
					break;
				case 0xA64F:		//	ꙏ	0xea 0x99 0x8f	CYRILLIC SMALL LETTER NEUTRAL YER
					*p = 0xA64E;	//	Ꙏ	0xea 0x99 0x8e	CYRILLIC CAPITAL LETTER NEUTRAL YER
					break;
				case 0xA651:		//	ꙑ	0xea 0x99 0x91	CYRILLIC SMALL LETTER YERU WITH BACK YER
					*p = 0xA650;	//	Ꙑ	0xea 0x99 0x90	CYRILLIC CAPITAL LETTER YERU WITH BACK YER
					break;
				case 0xA653:		//	ꙓ	0xea 0x99 0x93	CYRILLIC SMALL LETTER IOTIFIED YAT
					*p = 0xA652;	//	Ꙓ	0xea 0x99 0x92	CYRILLIC CAPITAL LETTER IOTIFIED YAT
					break;
				case 0xA655:		//	ꙕ	0xea 0x99 0x95	CYRILLIC SMALL LETTER REVERSED YU
					*p = 0xA654;	//	Ꙕ	0xea 0x99 0x94	CYRILLIC CAPITAL LETTER REVERSED YU
					break;
				case 0xA657:		//	ꙗ	0xea 0x99 0x97	CYRILLIC SMALL LETTER IOTIFIED A
					*p = 0xA656;	//	Ꙗ	0xea 0x99 0x96	CYRILLIC CAPITAL LETTER IOTIFIED A
					break;
				case 0xA659:		//	ꙙ	0xea 0x99 0x99	CYRILLIC SMALL LETTER CLOSED LITTLE YUS
					*p = 0xA658;	//	Ꙙ	0xea 0x99 0x98	CYRILLIC CAPITAL LETTER CLOSED LITTLE YUS
					break;
				case 0xA65B:		//	ꙛ	0xea 0x99 0x9b	CYRILLIC SMALL LETTER BLENDED YUS
					*p = 0xA65A;	//	Ꙛ	0xea 0x99 0x9a	CYRILLIC CAPITAL LETTER BLENDED YUS
					break;
				case 0xA65D:		//	ꙝ	0xea 0x99 0x9d	CYRILLIC SMALL LETTER IOTIFIED CLOSED LITTLE YUS
					*p = 0xA65C;	//	Ꙝ	0xea 0x99 0x9c	CYRILLIC CAPITAL LETTER IOTIFIED CLOSED LITTLE YUS
					break;
				case 0xA65F:		//	ꙟ	0xea 0x99 0x9f	CYRILLIC SMALL LETTER YN
					*p = 0xA65E;	//	Ꙟ	0xea 0x99 0x9e	CYRILLIC CAPITAL LETTER YN
					break;
				case 0xA661:		//	ꙡ	0xea 0x99 0xa1	CYRILLIC SMALL LETTER REVERSED TSE
					*p = 0xA660;	//	Ꙡ	0xea 0x99 0xa0	CYRILLIC CAPITAL LETTER REVERSED TSE
					break;
				case 0xA663:		//	ꙣ	0xea 0x99 0xa3	CYRILLIC SMALL LETTER SOFT DE
					*p = 0xA662;	//	Ꙣ	0xea 0x99 0xa2	CYRILLIC CAPITAL LETTER SOFT DE
					break;
				case 0xA665:		//	ꙥ	0xea 0x99 0xa5	CYRILLIC SMALL LETTER SOFT EL
					*p = 0xA664;	//	Ꙥ	0xea 0x99 0xa4	CYRILLIC CAPITAL LETTER SOFT EL
					break;
				case 0xA667:		//	ꙧ	0xea 0x99 0xa7	CYRILLIC SMALL LETTER SOFT EM
					*p = 0xA666;	//	Ꙧ	0xea 0x99 0xa6	CYRILLIC CAPITAL LETTER SOFT EM
					break;
				case 0xA669:		//	ꙩ	0xea 0x99 0xa9	CYRILLIC SMALL LETTER MONOCULAR O
					*p = 0xA668;	//	Ꙩ	0xea 0x99 0xa8	CYRILLIC CAPITAL LETTER MONOCULAR O
					break;
				case 0xA66B:		//	ꙫ	0xea 0x99 0xab	CYRILLIC SMALL LETTER BINOCULAR O
					*p = 0xA66A;	//	Ꙫ	0xea 0x99 0xaa	CYRILLIC CAPITAL LETTER BINOCULAR O
					break;
				case 0xA66D:		//	ꙭ	0xea 0x99 0xad	CYRILLIC SMALL LETTER DOUBLE MONOCULAR O
					*p = 0xA66C;	//	Ꙭ	0xea 0x99 0xac	CYRILLIC CAPITAL LETTER DOUBLE MONOCULAR O
					break;
				case 0xA681:		//	ꚁ	0xea 0x9a 0x81	CYRILLIC SMALL LETTER DWE
					*p = 0xA680;	//	Ꚁ	0xea 0x9a 0x80	CYRILLIC CAPITAL LETTER DWE
					break;
				case 0xA683:		//	ꚃ	0xea 0x9a 0x83	CYRILLIC SMALL LETTER DZWE
					*p = 0xA682;	//	Ꚃ	0xea 0x9a 0x82	CYRILLIC CAPITAL LETTER DZWE
					break;
				case 0xA685:		//	ꚅ	0xea 0x9a 0x85	CYRILLIC SMALL LETTER ZHWE
					*p = 0xA684;	//	Ꚅ	0xea 0x9a 0x84	CYRILLIC CAPITAL LETTER ZHWE
					break;
				case 0xA687:		//	ꚇ	0xea 0x9a 0x87	CYRILLIC SMALL LETTER CCHE
					*p = 0xA686;	//	Ꚇ	0xea 0x9a 0x86	CYRILLIC CAPITAL LETTER CCHE
					break;
				case 0xA689:		//	ꚉ	0xea 0x9a 0x89	CYRILLIC SMALL LETTER DZZE
					*p = 0xA688;	//	Ꚉ	0xea 0x9a 0x88	CYRILLIC CAPITAL LETTER DZZE
					break;
				case 0xA68B:		//	ꚋ	0xea 0x9a 0x8b	CYRILLIC SMALL LETTER TE WITH MIDDLE HOOK
					*p = 0xA68A;	//	Ꚋ	0xea 0x9a 0x8a	CYRILLIC CAPITAL LETTER TE WITH MIDDLE HOOK
					break;
				case 0xA68D:		//	ꚍ	0xea 0x9a 0x8d	CYRILLIC SMALL LETTER TWE
					*p = 0xA68C;	//	Ꚍ	0xea 0x9a 0x8c	CYRILLIC CAPITAL LETTER TWE
					break;
				case 0xA68F:		//	ꚏ	0xea 0x9a 0x8f	CYRILLIC SMALL LETTER TSWE
					*p = 0xA68E;	//	Ꚏ	0xea 0x9a 0x8e	CYRILLIC CAPITAL LETTER TSWE
					break;
				case 0xA691:		//	ꚑ	0xea 0x9a 0x91	CYRILLIC SMALL LETTER TSSE
					*p = 0xA690;	//	Ꚑ	0xea 0x9a 0x90	CYRILLIC CAPITAL LETTER TSSE
					break;
				case 0xA693:		//	ꚓ	0xea 0x9a 0x93	CYRILLIC SMALL LETTER TCHE
					*p = 0xA692;	//	Ꚓ	0xea 0x9a 0x92	CYRILLIC CAPITAL LETTER TCHE
					break;
				case 0xA695:		//	ꚕ	0xea 0x9a 0x95	CYRILLIC SMALL LETTER HWE
					*p = 0xA694;	//	Ꚕ	0xea 0x9a 0x94	CYRILLIC CAPITAL LETTER HWE
					break;
				case 0xA697:		//	ꚗ	0xea 0x9a 0x97	CYRILLIC SMALL LETTER SHWE
					*p = 0xA696;	//	Ꚗ	0xea 0x9a 0x96	CYRILLIC CAPITAL LETTER SHWE
					break;
				case 0xA699:		//	ꚙ	0xea 0x9a 0x99	CYRILLIC SMALL LETTER DOUBLE O
					*p = 0xA698;	//	Ꚙ	0xea 0x9a 0x98	CYRILLIC CAPITAL LETTER DOUBLE O
					break;
				case 0xA69B:		//	ꚛ	0xea 0x9a 0x9b	CYRILLIC SMALL LETTER CROSSED O
					*p = 0xA69A;	//	Ꚛ	0xea 0x9a 0x9a	CYRILLIC CAPITAL LETTER CROSSED O
					break;
				case 0xA723:		//	ꜣ	0xea 0x9c 0xa3	LATIN SMALL LETTER EGYPTOLOGICAL ALEF
					*p = 0xA722;	//	Ꜣ	0xea 0x9c 0xa2	LATIN CAPITAL LETTER EGYPTOLOGICAL ALEF
					break;
				case 0xA725:		//	ꜥ	0xea 0x9c 0xa5	LATIN SMALL LETTER EGYPTOLOGICAL AIN
					*p = 0xA724;	//	Ꜥ	0xea 0x9c 0xa4	LATIN CAPITAL LETTER EGYPTOLOGICAL AIN
					break;
				case 0xA727:		//	ꜧ	0xea 0x9c 0xa7	LATIN SMALL LETTER HENG
					*p = 0xA726;	//	Ꜧ	0xea 0x9c 0xa6	LATIN CAPITAL LETTER HENG
					break;
				case 0xA729:		//	ꜩ	0xea 0x9c 0xa9	LATIN SMALL LETTER TZ
					*p = 0xA728;	//	Ꜩ	0xea 0x9c 0xa8	LATIN CAPITAL LETTER TZ
					break;
				case 0xA72B:		//	ꜫ	0xea 0x9c 0xab	LATIN SMALL LETTER TRESILLO
					*p = 0xA72A;	//	Ꜫ	0xea 0x9c 0xaa	LATIN CAPITAL LETTER TRESILLO
					break;
				case 0xA72D:		//	ꜭ	0xea 0x9c 0xad	LATIN SMALL LETTER CUATRILLO
					*p = 0xA72C;	//	Ꜭ	0xea 0x9c 0xac	LATIN CAPITAL LETTER CUATRILLO
					break;
				case 0xA72F:		//	ꜯ	0xea 0x9c 0xaf	LATIN SMALL LETTER CUATRILLO WITH COMMA
					*p = 0xA72E;	//	Ꜯ	0xea 0x9c 0xae	LATIN CAPITAL LETTER CUATRILLO WITH COMMA
					break;
				case 0xA733:		//	ꜳ	0xea 0x9c 0xb3	LATIN SMALL LETTER AA
					*p = 0xA732;	//	Ꜳ	0xea 0x9c 0xb2	LATIN CAPITAL LETTER AA
					break;
				case 0xA735:		//	ꜵ	0xea 0x9c 0xb5	LATIN SMALL LETTER AO
					*p = 0xA734;	//	Ꜵ	0xea 0x9c 0xb4	LATIN CAPITAL LETTER AO
					break;
				case 0xA737:		//	ꜷ	0xea 0x9c 0xb7	LATIN SMALL LETTER AU
					*p = 0xA736;	//	Ꜷ	0xea 0x9c 0xb6	LATIN CAPITAL LETTER AU
					break;
				case 0xA739:		//	ꜹ	0xea 0x9c 0xb9	LATIN SMALL LETTER AV
					*p = 0xA738;	//	Ꜹ	0xea 0x9c 0xb8	LATIN CAPITAL LETTER AV
					break;
				case 0xA73B:		//	ꜻ	0xea 0x9c 0xbb	LATIN SMALL LETTER AV WITH HORIZONTAL BAR
					*p = 0xA73A;	//	Ꜻ	0xea 0x9c 0xba	LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR
					break;
				case 0xA73D:		//	ꜽ	0xea 0x9c 0xbd	LATIN SMALL LETTER AY
					*p = 0xA73C;	//	Ꜽ	0xea 0x9c 0xbc	LATIN CAPITAL LETTER AY
					break;
				case 0xA73F:		//	ꜿ	0xea 0x9c 0xbf	LATIN SMALL LETTER REVERSED C WITH DOT
					*p = 0xA73E;	//	Ꜿ	0xea 0x9c 0xbe	LATIN CAPITAL LETTER REVERSED C WITH DOT
					break;
				case 0xA741:		//	ꝁ	0xea 0x9d 0x81	LATIN SMALL LETTER K WITH STROKE
					*p = 0xA740;	//	Ꝁ	0xea 0x9d 0x80	LATIN CAPITAL LETTER K WITH STROKE
					break;
				case 0xA743:		//	ꝃ	0xea 0x9d 0x83	LATIN SMALL LETTER K WITH DIAGONAL STROKE
					*p = 0xA742;	//	Ꝃ	0xea 0x9d 0x82	LATIN CAPITAL LETTER K WITH DIAGONAL STROKE
					break;
				case 0xA745:		//	ꝅ	0xea 0x9d 0x85	LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE
					*p = 0xA744;	//	Ꝅ	0xea 0x9d 0x84	LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE
					break;
				case 0xA747:		//	ꝇ	0xea 0x9d 0x87	LATIN SMALL LETTER BROKEN L
					*p = 0xA746;	//	Ꝇ	0xea 0x9d 0x86	LATIN CAPITAL LETTER BROKEN L
					break;
				case 0xA749:		//	ꝉ	0xea 0x9d 0x89	LATIN SMALL LETTER L WITH HIGH STROKE
					*p = 0xA748;	//	Ꝉ	0xea 0x9d 0x88	LATIN CAPITAL LETTER L WITH HIGH STROKE
					break;
				case 0xA74B:		//	ꝋ	0xea 0x9d 0x8b	LATIN SMALL LETTER O WITH LONG STROKE OVERLAY
					*p = 0xA74A;	//	Ꝋ	0xea 0x9d 0x8a	LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY
					break;
				case 0xA74D:		//	ꝍ	0xea 0x9d 0x8d	LATIN SMALL LETTER O WITH LOOP
					*p = 0xA74C;	//	Ꝍ	0xea 0x9d 0x8c	LATIN CAPITAL LETTER O WITH LOOP
					break;
				case 0xA74F:		//	ꝏ	0xea 0x9d 0x8f	LATIN SMALL LETTER OO
					*p = 0xA74E;	//	Ꝏ	0xea 0x9d 0x8e	LATIN CAPITAL LETTER OO
					break;
				case 0xA751:		//	ꝑ	0xea 0x9d 0x91	LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER
					*p = 0xA750;	//	Ꝑ	0xea 0x9d 0x90	LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER
					break;
				case 0xA753:		//	ꝓ	0xea 0x9d 0x93	LATIN SMALL LETTER P WITH FLOURISH
					*p = 0xA752;	//	Ꝓ	0xea 0x9d 0x92	LATIN CAPITAL LETTER P WITH FLOURISH
					break;
				case 0xA755:		//	ꝕ	0xea 0x9d 0x95	LATIN SMALL LETTER P WITH SQUIRREL TAIL
					*p = 0xA754;	//	Ꝕ	0xea 0x9d 0x94	LATIN CAPITAL LETTER P WITH SQUIRREL TAIL
					break;
				case 0xA757:		//	ꝗ	0xea 0x9d 0x97	LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER
					*p = 0xA756;	//	Ꝗ	0xea 0x9d 0x96	LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER
					break;
				case 0xA759:		//	ꝙ	0xea 0x9d 0x99	LATIN SMALL LETTER Q WITH DIAGONAL STROKE
					*p = 0xA758;	//	Ꝙ	0xea 0x9d 0x98	LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE
					break;
				case 0xA75B:		//	ꝛ	0xea 0x9d 0x9b	LATIN SMALL LETTER R ROTUNDA
					*p = 0xA75A;	//	Ꝛ	0xea 0x9d 0x9a	LATIN CAPITAL LETTER R ROTUNDA
					break;
				case 0xA75D:		//	ꝝ	0xea 0x9d 0x9d	LATIN SMALL LETTER RUM ROTUNDA
					*p = 0xA75C;	//	Ꝝ	0xea 0x9d 0x9c	LATIN CAPITAL LETTER RUM ROTUNDA
					break;
				case 0xA75F:		//	ꝟ	0xea 0x9d 0x9f	LATIN SMALL LETTER V WITH DIAGONAL STROKE
					*p = 0xA75E;	//	Ꝟ	0xea 0x9d 0x9e	LATIN CAPITAL LETTER V WITH DIAGONAL STROKE
					break;
				case 0xA761:		//	ꝡ	0xea 0x9d 0xa1	LATIN SMALL LETTER VY
					*p = 0xA760;	//	Ꝡ	0xea 0x9d 0xa0	LATIN CAPITAL LETTER VY
					break;
				case 0xA763:		//	ꝣ	0xea 0x9d 0xa3	LATIN SMALL LETTER VISIGOTHIC Z
					*p = 0xA762;	//	Ꝣ	0xea 0x9d 0xa2	LATIN CAPITAL LETTER VISIGOTHIC Z
					break;
				case 0xA765:		//	ꝥ	0xea 0x9d 0xa5	LATIN SMALL LETTER THORN WITH STROKE
					*p = 0xA764;	//	Ꝥ	0xea 0x9d 0xa4	LATIN CAPITAL LETTER THORN WITH STROKE
					break;
				case 0xA767:		//	ꝧ	0xea 0x9d 0xa7	LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER
					*p = 0xA766;	//	Ꝧ	0xea 0x9d 0xa6	LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER
					break;
				case 0xA769:		//	ꝩ	0xea 0x9d 0xa9	LATIN SMALL LETTER VEND
					*p = 0xA768;	//	Ꝩ	0xea 0x9d 0xa8	LATIN CAPITAL LETTER VEND
					break;
				case 0xA76B:		//	ꝫ	0xea 0x9d 0xab	LATIN SMALL LETTER ET
					*p = 0xA76A;	//	Ꝫ	0xea 0x9d 0xaa	LATIN CAPITAL LETTER ET
					break;
				case 0xA76D:		//	ꝭ	0xea 0x9d 0xad	LATIN SMALL LETTER IS
					*p = 0xA76C;	//	Ꝭ	0xea 0x9d 0xac	LATIN CAPITAL LETTER IS
					break;
				case 0xA76F:		//	ꝯ	0xea 0x9d 0xaf	LATIN SMALL LETTER CON
					*p = 0xA76E;	//	Ꝯ	0xea 0x9d 0xae	LATIN CAPITAL LETTER CON
					break;
				case 0xA77A:		//	ꝺ	0xea 0x9d 0xba	LATIN SMALL LETTER INSULAR D
					*p = 0xA779;	//	Ꝺ	0xea 0x9d 0xb9	LATIN CAPITAL LETTER INSULAR D
					break;
				case 0xA77C:		//	ꝼ	0xea 0x9d 0xbc	LATIN SMALL LETTER INSULAR F
					*p = 0xA77B;	//	Ꝼ	0xea 0x9d 0xbb	LATIN CAPITAL LETTER INSULAR F
					break;
				case 0xA77F:		//	ꝿ	0xea 0x9d 0xbf	LATIN SMALL LETTER TURNED INSULAR G
					*p = 0xA77E;	//	Ꝿ	0xea 0x9d 0xbe	LATIN CAPITAL LETTER TURNED INSULAR G
					break;
				case 0xA781:		//	ꞁ	0xea 0x9e 0x81	LATIN SMALL LETTER TURNED L
					*p = 0xA780;	//	Ꞁ	0xea 0x9e 0x80	LATIN CAPITAL LETTER TURNED L
					break;
				case 0xA783:		//	ꞃ	0xea 0x9e 0x83	LATIN SMALL LETTER INSULAR R
					*p = 0xA782;	//	Ꞃ	0xea 0x9e 0x82	LATIN CAPITAL LETTER INSULAR R
					break;
				case 0xA785:		//	ꞅ	0xea 0x9e 0x85	LATIN SMALL LETTER INSULAR S
					*p = 0xA784;	//	Ꞅ	0xea 0x9e 0x84	LATIN CAPITAL LETTER INSULAR S
					break;
				case 0xA787:		//	ꞇ	0xea 0x9e 0x87	LATIN SMALL LETTER INSULAR T
					*p = 0xA786;	//	Ꞇ	0xea 0x9e 0x86	LATIN CAPITAL LETTER INSULAR T
					break;
				case 0xA78C:		//	ꞌ	0xea 0x9e 0x8c	LATIN SMALL LETTER SALTILLO
					*p = 0xA78B;	//	Ꞌ	0xea 0x9e 0x8b	LATIN CAPITAL LETTER SALTILLO
					break;
				case 0xA791:		//	ꞑ	0xea 0x9e 0x91	LATIN SMALL LETTER N WITH DESCENDER
					*p = 0xA790;	//	Ꞑ	0xea 0x9e 0x90	LATIN CAPITAL LETTER N WITH DESCENDER
					break;
				case 0xA793:		//	ꞓ	0xea 0x9e 0x93	LATIN SMALL LETTER C WITH BAR
					*p = 0xA792;	//	Ꞓ	0xea 0x9e 0x92	LATIN CAPITAL LETTER C WITH BAR
					break;
				case 0xA794:		//	ꞔ	0xea 0x9e 0x94	LATIN SMALL LETTER C WITH PALATAL HOOK
					*p = 0xA7C4;	//	Ꞔ	0xea 0x9f 0x84	LATIN CAPITAL LETTER C WITH PALATAL HOOK
					break;
				case 0xA797:		//	ꞗ	0xea 0x9e 0x97	LATIN SMALL LETTER B WITH FLOURISH
					*p = 0xA796;	//	Ꞗ	0xea 0x9e 0x96	LATIN CAPITAL LETTER B WITH FLOURISH
					break;
				case 0xA799:		//	ꞙ	0xea 0x9e 0x99	LATIN SMALL LETTER F WITH STROKE
					*p = 0xA798;	//	Ꞙ	0xea 0x9e 0x98	LATIN CAPITAL LETTER F WITH STROKE
					break;
				case 0xA79B:		//	ꞛ	0xea 0x9e 0x9b	LATIN SMALL LETTER VOLAPUK AE
					*p = 0xA79A;	//	Ꞛ	0xea 0x9e 0x9a	LATIN CAPITAL LETTER VOLAPUK AE
					break;
				case 0xA79D:		//	ꞝ	0xea 0x9e 0x9d	LATIN SMALL LETTER VOLAPUK OE
					*p = 0xA79C;	//	Ꞝ	0xea 0x9e 0x9c	LATIN CAPITAL LETTER VOLAPUK OE
					break;
				case 0xA79F:		//	ꞟ	0xea 0x9e 0x9f	LATIN SMALL LETTER VOLAPUK UE
					*p = 0xA79E;	//	Ꞟ	0xea 0x9e 0x9e	LATIN CAPITAL LETTER VOLAPUK UE
					break;
				case 0xA7A1:		//	ꞡ	0xea 0x9e 0xa1	LATIN SMALL LETTER G WITH OBLIQUE STROKE
					*p = 0xA7A0;	//	Ꞡ	0xea 0x9e 0xa0	LATIN CAPITAL LETTER G WITH OBLIQUE STROKE
					break;
				case 0xA7A3:		//	ꞣ	0xea 0x9e 0xa3	LATIN SMALL LETTER K WITH OBLIQUE STROKE
					*p = 0xA7A2;	//	Ꞣ	0xea 0x9e 0xa2	LATIN CAPITAL LETTER K WITH OBLIQUE STROKE
					break;
				case 0xA7A5:		//	ꞥ	0xea 0x9e 0xa5	LATIN SMALL LETTER N WITH OBLIQUE STROKE
					*p = 0xA7A4;	//	Ꞥ	0xea 0x9e 0xa4	LATIN CAPITAL LETTER N WITH OBLIQUE STROKE
					break;
				case 0xA7A7:		//	ꞧ	0xea 0x9e 0xa7	LATIN SMALL LETTER R WITH OBLIQUE STROKE
					*p = 0xA7A6;	//	Ꞧ	0xea 0x9e 0xa6	LATIN CAPITAL LETTER R WITH OBLIQUE STROKE
					break;
				case 0xA7A9:		//	ꞩ	0xea 0x9e 0xa9	LATIN SMALL LETTER S WITH OBLIQUE STROKE
					*p = 0xA7A8;	//	Ꞩ	0xea 0x9e 0xa8	LATIN CAPITAL LETTER S WITH OBLIQUE STROKE
					break;
				case 0xA7B5:		//	ꞵ	0xea 0x9e 0xb5	LATIN SMALL LETTER BETA
					*p = 0xA7B4;	//	Ꞵ	0xea 0x9e 0xb4	LATIN CAPITAL LETTER BETA
					break;
				case 0xA7B7:		//	ꞷ	0xea 0x9e 0xb7	LATIN SMALL LETTER OMEGA
					*p = 0xA7B6;	//	Ꞷ	0xea 0x9e 0xb6	LATIN CAPITAL LETTER OMEGA
					break;
				case 0xA7B9:		//	ꞹ	0xea 0x9e 0xb9	LATIN SMALL LETTER U WITH STROKE
					*p = 0xA7B8;	//	Ꞹ	0xea 0x9e 0xb8	LATIN CAPITAL LETTER U WITH STROKE
					break;
				case 0xA7BB:		//	ꞻ	0xea 0x9e 0xbb	LATIN SMALL LETTER GLOTTAL A
					*p = 0xA7BA;	//	Ꞻ	0xea 0x9e 0xba	LATIN CAPITAL LETTER GLOTTAL A
					break;
				case 0xA7BD:		//	ꞽ	0xea 0x9e 0xbd	LATIN SMALL LETTER GLOTTAL I
					*p = 0xA7BC;	//	Ꞽ	0xea 0x9e 0xbc	LATIN CAPITAL LETTER GLOTTAL I
					break;
				case 0xA7BF:		//	ꞿ	0xea 0x9e 0xbf	LATIN SMALL LETTER GLOTTAL U
					*p = 0xA7BE;	//	Ꞿ	0xea 0x9e 0xbe	LATIN CAPITAL LETTER GLOTTAL U
					break;
				case 0xA7C3:		//	ꟃ	0xea 0x9f 0x83	LATIN SMALL LETTER ANGLICANA W
					*p = 0xA7C2;	//	Ꟃ	0xea 0x9f 0x82	LATIN CAPITAL LETTER ANGLICANA W
					break;
				case 0xA7C8:		//	ꟈ	0xea 0x9f 0x88	LATIN SMALL LETTER D WITH SHORT STROKE OVERLAY
					*p = 0xA7C7;	//	Ꟈ	0xea 0x9f 0x87	LATIN CAPITAL LETTER D WITH SHORT STROKE OVERLAY
					break;
				case 0xA7CA:		//	ꟊ	0xea 0x9f 0x8a	LATIN SMALL LETTER S WITH SHORT STROKE OVERLAY
					*p = 0xA7C9;	//	Ꟊ	0xea 0x9f 0x89	LATIN CAPITAL LETTER S WITH SHORT STROKE OVERLAY
					break;
				case 0xA7F6:		//	ꟶ	0xea 0x9f 0xb6	LATIN SMALL LETTER REVERSED HALF H
					*p = 0xA7F5;	//	Ꟶ	0xea 0x9f 0xb5	LATIN CAPITAL LETTER REVERSED HALF H
					break;
				case 0xAB53:		//	ꭓ	0xea 0xad 0x93	LATIN SMALL LETTER CHI
					*p = 0xA7B3;	//	Ꭓ	0xea 0x9e 0xb3	LATIN CAPITAL LETTER CHI
					break;
				case 0xAB70:		//	ꭰ	0xea 0xad 0xb0	CHEROKEE SMALL LETTER A
					*p = 0x13A0;	//	Ꭰ	0xe1 0x8e 0xa0	CHEROKEE CAPITAL LETTER A
					break;
				case 0xAB71:		//	ꭱ	0xea 0xad 0xb1	CHEROKEE SMALL LETTER E
					*p = 0x13A1;	//	Ꭱ	0xe1 0x8e 0xa1	CHEROKEE CAPITAL LETTER E
					break;
				case 0xAB72:		//	ꭲ	0xea 0xad 0xb2	CHEROKEE SMALL LETTER I
					*p = 0x13A2;	//	Ꭲ	0xe1 0x8e 0xa2	CHEROKEE CAPITAL LETTER I
					break;
				case 0xAB73:		//	ꭳ	0xea 0xad 0xb3	CHEROKEE SMALL LETTER O
					*p = 0x13A3;	//	Ꭳ	0xe1 0x8e 0xa3	CHEROKEE CAPITAL LETTER O
					break;
				case 0xAB74:		//	ꭴ	0xea 0xad 0xb4	CHEROKEE SMALL LETTER U
					*p = 0x13A4;	//	Ꭴ	0xe1 0x8e 0xa4	CHEROKEE CAPITAL LETTER U
					break;
				case 0xAB75:		//	ꭵ	0xea 0xad 0xb5	CHEROKEE SMALL LETTER V
					*p = 0x13A5;	//	Ꭵ	0xe1 0x8e 0xa5	CHEROKEE CAPITAL LETTER V
					break;
				case 0xAB76:		//	ꭶ	0xea 0xad 0xb6	CHEROKEE SMALL LETTER GA
					*p = 0x13A6;	//	Ꭶ	0xe1 0x8e 0xa6	CHEROKEE CAPITAL LETTER GA
					break;
				case 0xAB77:		//	ꭷ	0xea 0xad 0xb7	CHEROKEE SMALL LETTER KA
					*p = 0x13A7;	//	Ꭷ	0xe1 0x8e 0xa7	CHEROKEE CAPITAL LETTER KA
					break;
				case 0xAB78:		//	ꭸ	0xea 0xad 0xb8	CHEROKEE SMALL LETTER GE
					*p = 0x13A8;	//	Ꭸ	0xe1 0x8e 0xa8	CHEROKEE CAPITAL LETTER GE
					break;
				case 0xAB79:		//	ꭹ	0xea 0xad 0xb9	CHEROKEE SMALL LETTER GI
					*p = 0x13A9;	//	Ꭹ	0xe1 0x8e 0xa9	CHEROKEE CAPITAL LETTER GI
					break;
				case 0xAB7A:		//	ꭺ	0xea 0xad 0xba	CHEROKEE SMALL LETTER GO
					*p = 0x13AA;	//	Ꭺ	0xe1 0x8e 0xaa	CHEROKEE CAPITAL LETTER GO
					break;
				case 0xAB7B:		//	ꭻ	0xea 0xad 0xbb	CHEROKEE SMALL LETTER GU
					*p = 0x13AB;	//	Ꭻ	0xe1 0x8e 0xab	CHEROKEE CAPITAL LETTER GU
					break;
				case 0xAB7C:		//	ꭼ	0xea 0xad 0xbc	CHEROKEE SMALL LETTER GV
					*p = 0x13AC;	//	Ꭼ	0xe1 0x8e 0xac	CHEROKEE CAPITAL LETTER GV
					break;
				case 0xAB7D:		//	ꭽ	0xea 0xad 0xbd	CHEROKEE SMALL LETTER HA
					*p = 0x13AD;	//	Ꭽ	0xe1 0x8e 0xad	CHEROKEE CAPITAL LETTER HA
					break;
				case 0xAB7E:		//	ꭾ	0xea 0xad 0xbe	CHEROKEE SMALL LETTER HE
					*p = 0x13AE;	//	Ꭾ	0xe1 0x8e 0xae	CHEROKEE CAPITAL LETTER HE
					break;
				case 0xAB7F:		//	ꭿ	0xea 0xad 0xbf	CHEROKEE SMALL LETTER HI
					*p = 0x13AF;	//	Ꭿ	0xe1 0x8e 0xaf	CHEROKEE CAPITAL LETTER HI
					break;
				case 0xAB80:		//	ꮀ	0xea 0xae 0x80	CHEROKEE SMALL LETTER HO
					*p = 0x13B0;	//	Ꮀ	0xe1 0x8e 0xb0	CHEROKEE CAPITAL LETTER HO
					break;
				case 0xAB81:		//	ꮁ	0xea 0xae 0x81	CHEROKEE SMALL LETTER HU
					*p = 0x13B1;	//	Ꮁ	0xe1 0x8e 0xb1	CHEROKEE CAPITAL LETTER HU
					break;
				case 0xAB82:		//	ꮂ	0xea 0xae 0x82	CHEROKEE SMALL LETTER HV
					*p = 0x13B2;	//	Ꮂ	0xe1 0x8e 0xb2	CHEROKEE CAPITAL LETTER HV
					break;
				case 0xAB83:		//	ꮃ	0xea 0xae 0x83	CHEROKEE SMALL LETTER LA
					*p = 0x13B3;	//	Ꮃ	0xe1 0x8e 0xb3	CHEROKEE CAPITAL LETTER LA
					break;
				case 0xAB84:		//	ꮄ	0xea 0xae 0x84	CHEROKEE SMALL LETTER LE
					*p = 0x13B4;	//	Ꮄ	0xe1 0x8e 0xb4	CHEROKEE CAPITAL LETTER LE
					break;
				case 0xAB85:		//	ꮅ	0xea 0xae 0x85	CHEROKEE SMALL LETTER LI
					*p = 0x13B5;	//	Ꮅ	0xe1 0x8e 0xb5	CHEROKEE CAPITAL LETTER LI
					break;
				case 0xAB86:		//	ꮆ	0xea 0xae 0x86	CHEROKEE SMALL LETTER LO
					*p = 0x13B6;	//	Ꮆ	0xe1 0x8e 0xb6	CHEROKEE CAPITAL LETTER LO
					break;
				case 0xAB87:		//	ꮇ	0xea 0xae 0x87	CHEROKEE SMALL LETTER LU
					*p = 0x13B7;	//	Ꮇ	0xe1 0x8e 0xb7	CHEROKEE CAPITAL LETTER LU
					break;
				case 0xAB88:		//	ꮈ	0xea 0xae 0x88	CHEROKEE SMALL LETTER LV
					*p = 0x13B8;	//	Ꮈ	0xe1 0x8e 0xb8	CHEROKEE CAPITAL LETTER LV
					break;
				case 0xAB89:		//	ꮉ	0xea 0xae 0x89	CHEROKEE SMALL LETTER MA
					*p = 0x13B9;	//	Ꮉ	0xe1 0x8e 0xb9	CHEROKEE CAPITAL LETTER MA
					break;
				case 0xAB8A:		//	ꮊ	0xea 0xae 0x8a	CHEROKEE SMALL LETTER ME
					*p = 0x13BA;	//	Ꮊ	0xe1 0x8e 0xba	CHEROKEE CAPITAL LETTER ME
					break;
				case 0xAB8B:		//	ꮋ	0xea 0xae 0x8b	CHEROKEE SMALL LETTER MI
					*p = 0x13BB;	//	Ꮋ	0xe1 0x8e 0xbb	CHEROKEE CAPITAL LETTER MI
					break;
				case 0xAB8C:		//	ꮌ	0xea 0xae 0x8c	CHEROKEE SMALL LETTER MO
					*p = 0x13BC;	//	Ꮌ	0xe1 0x8e 0xbc	CHEROKEE CAPITAL LETTER MO
					break;
				case 0xAB8D:		//	ꮍ	0xea 0xae 0x8d	CHEROKEE SMALL LETTER MU
					*p = 0x13BD;	//	Ꮍ	0xe1 0x8e 0xbd	CHEROKEE CAPITAL LETTER MU
					break;
				case 0xAB8E:		//	ꮎ	0xea 0xae 0x8e	CHEROKEE SMALL LETTER NA
					*p = 0x13BE;	//	Ꮎ	0xe1 0x8e 0xbe	CHEROKEE CAPITAL LETTER NA
					break;
				case 0xAB8F:		//	ꮏ	0xea 0xae 0x8f	CHEROKEE SMALL LETTER HNA
					*p = 0x13BF;	//	Ꮏ	0xe1 0x8e 0xbf	CHEROKEE CAPITAL LETTER HNA
					break;
				case 0xAB90:		//	ꮐ	0xea 0xae 0x90	CHEROKEE SMALL LETTER NAH
					*p = 0x13C0;	//	Ꮐ	0xe1 0x8f 0x80	CHEROKEE CAPITAL LETTER NAH
					break;
				case 0xAB91:		//	ꮑ	0xea 0xae 0x91	CHEROKEE SMALL LETTER NE
					*p = 0x13C1;	//	Ꮑ	0xe1 0x8f 0x81	CHEROKEE CAPITAL LETTER NE
					break;
				case 0xAB92:		//	ꮒ	0xea 0xae 0x92	CHEROKEE SMALL LETTER NI
					*p = 0x13C2;	//	Ꮒ	0xe1 0x8f 0x82	CHEROKEE CAPITAL LETTER NI
					break;
				case 0xAB93:		//	ꮓ	0xea 0xae 0x93	CHEROKEE SMALL LETTER NO
					*p = 0x13C3;	//	Ꮓ	0xe1 0x8f 0x83	CHEROKEE CAPITAL LETTER NO
					break;
				case 0xAB94:		//	ꮔ	0xea 0xae 0x94	CHEROKEE SMALL LETTER NU
					*p = 0x13C4;	//	Ꮔ	0xe1 0x8f 0x84	CHEROKEE CAPITAL LETTER NU
					break;
				case 0xAB95:		//	ꮕ	0xea 0xae 0x95	CHEROKEE SMALL LETTER NV
					*p = 0x13C5;	//	Ꮕ	0xe1 0x8f 0x85	CHEROKEE CAPITAL LETTER NV
					break;
				case 0xAB96:		//	ꮖ	0xea 0xae 0x96	CHEROKEE SMALL LETTER QUA
					*p = 0x13C6;	//	Ꮖ	0xe1 0x8f 0x86	CHEROKEE CAPITAL LETTER QUA
					break;
				case 0xAB97:		//	ꮗ	0xea 0xae 0x97	CHEROKEE SMALL LETTER QUE
					*p = 0x13C7;	//	Ꮗ	0xe1 0x8f 0x87	CHEROKEE CAPITAL LETTER QUE
					break;
				case 0xAB98:		//	ꮘ	0xea 0xae 0x98	CHEROKEE SMALL LETTER QUI
					*p = 0x13C8;	//	Ꮘ	0xe1 0x8f 0x88	CHEROKEE CAPITAL LETTER QUI
					break;
				case 0xAB99:		//	ꮙ	0xea 0xae 0x99	CHEROKEE SMALL LETTER QUO
					*p = 0x13C9;	//	Ꮙ	0xe1 0x8f 0x89	CHEROKEE CAPITAL LETTER QUO
					break;
				case 0xAB9A:		//	ꮚ	0xea 0xae 0x9a	CHEROKEE SMALL LETTER QUU
					*p = 0x13CA;	//	Ꮚ	0xe1 0x8f 0x8a	CHEROKEE CAPITAL LETTER QUU
					break;
				case 0xAB9B:		//	ꮛ	0xea 0xae 0x9b	CHEROKEE SMALL LETTER QUV
					*p = 0x13CB;	//	Ꮛ	0xe1 0x8f 0x8b	CHEROKEE CAPITAL LETTER QUV
					break;
				case 0xAB9C:		//	ꮜ	0xea 0xae 0x9c	CHEROKEE SMALL LETTER SA
					*p = 0x13CC;	//	Ꮜ	0xe1 0x8f 0x8c	CHEROKEE CAPITAL LETTER SA
					break;
				case 0xAB9D:		//	ꮝ	0xea 0xae 0x9d	CHEROKEE SMALL LETTER S
					*p = 0x13CD;	//	Ꮝ	0xe1 0x8f 0x8d	CHEROKEE CAPITAL LETTER S
					break;
				case 0xAB9E:		//	ꮞ	0xea 0xae 0x9e	CHEROKEE SMALL LETTER SE
					*p = 0x13CE;	//	Ꮞ	0xe1 0x8f 0x8e	CHEROKEE CAPITAL LETTER SE
					break;
				case 0xAB9F:		//	ꮟ	0xea 0xae 0x9f	CHEROKEE SMALL LETTER SI
					*p = 0x13CF;	//	Ꮟ	0xe1 0x8f 0x8f	CHEROKEE CAPITAL LETTER SI
					break;
				case 0xABA0:		//	ꮠ	0xea 0xae 0xa0	CHEROKEE SMALL LETTER SO
					*p = 0x13D0;	//	Ꮠ	0xe1 0x8f 0x90	CHEROKEE CAPITAL LETTER SO
					break;
				case 0xABA1:		//	ꮡ	0xea 0xae 0xa1	CHEROKEE SMALL LETTER SU
					*p = 0x13D1;	//	Ꮡ	0xe1 0x8f 0x91	CHEROKEE CAPITAL LETTER SU
					break;
				case 0xABA2:		//	ꮢ	0xea 0xae 0xa2	CHEROKEE SMALL LETTER SV
					*p = 0x13D2;	//	Ꮢ	0xe1 0x8f 0x92	CHEROKEE CAPITAL LETTER SV
					break;
				case 0xABA3:		//	ꮣ	0xea 0xae 0xa3	CHEROKEE SMALL LETTER DA
					*p = 0x13D3;	//	Ꮣ	0xe1 0x8f 0x93	CHEROKEE CAPITAL LETTER DA
					break;
				case 0xABA4:		//	ꮤ	0xea 0xae 0xa4	CHEROKEE SMALL LETTER TA
					*p = 0x13D4;	//	Ꮤ	0xe1 0x8f 0x94	CHEROKEE CAPITAL LETTER TA
					break;
				case 0xABA5:		//	ꮥ	0xea 0xae 0xa5	CHEROKEE SMALL LETTER DE
					*p = 0x13D5;	//	Ꮥ	0xe1 0x8f 0x95	CHEROKEE CAPITAL LETTER DE
					break;
				case 0xABA6:		//	ꮦ	0xea 0xae 0xa6	CHEROKEE SMALL LETTER TE
					*p = 0x13D6;	//	Ꮦ	0xe1 0x8f 0x96	CHEROKEE CAPITAL LETTER TE
					break;
				case 0xABA7:		//	ꮧ	0xea 0xae 0xa7	CHEROKEE SMALL LETTER DI
					*p = 0x13D7;	//	Ꮧ	0xe1 0x8f 0x97	CHEROKEE CAPITAL LETTER DI
					break;
				case 0xABA8:		//	ꮨ	0xea 0xae 0xa8	CHEROKEE SMALL LETTER TI
					*p = 0x13D8;	//	Ꮨ	0xe1 0x8f 0x98	CHEROKEE CAPITAL LETTER TI
					break;
				case 0xABA9:		//	ꮩ	0xea 0xae 0xa9	CHEROKEE SMALL LETTER DO
					*p = 0x13D9;	//	Ꮩ	0xe1 0x8f 0x99	CHEROKEE CAPITAL LETTER DO
					break;
				case 0xABAA:		//	ꮪ	0xea 0xae 0xaa	CHEROKEE SMALL LETTER DU
					*p = 0x13DA;	//	Ꮪ	0xe1 0x8f 0x9a	CHEROKEE CAPITAL LETTER DU
					break;
				case 0xABAB:		//	ꮫ	0xea 0xae 0xab	CHEROKEE SMALL LETTER DV
					*p = 0x13DB;	//	Ꮫ	0xe1 0x8f 0x9b	CHEROKEE CAPITAL LETTER DV
					break;
				case 0xABAC:		//	ꮬ	0xea 0xae 0xac	CHEROKEE SMALL LETTER DLA
					*p = 0x13DC;	//	Ꮬ	0xe1 0x8f 0x9c	CHEROKEE CAPITAL LETTER DLA
					break;
				case 0xABAD:		//	ꮭ	0xea 0xae 0xad	CHEROKEE SMALL LETTER TLA
					*p = 0x13DD;	//	Ꮭ	0xe1 0x8f 0x9d	CHEROKEE CAPITAL LETTER TLA
					break;
				case 0xABAE:		//	ꮮ	0xea 0xae 0xae	CHEROKEE SMALL LETTER TLE
					*p = 0x13DE;	//	Ꮮ	0xe1 0x8f 0x9e	CHEROKEE CAPITAL LETTER TLE
					break;
				case 0xABAF:		//	ꮯ	0xea 0xae 0xaf	CHEROKEE SMALL LETTER TLI
					*p = 0x13DF;	//	Ꮯ	0xe1 0x8f 0x9f	CHEROKEE CAPITAL LETTER TLI
					break;
				case 0xABB0:		//	ꮰ	0xea 0xae 0xb0	CHEROKEE SMALL LETTER TLO
					*p = 0x13E0;	//	Ꮰ	0xe1 0x8f 0xa0	CHEROKEE CAPITAL LETTER TLO
					break;
				case 0xABB1:		//	ꮱ	0xea 0xae 0xb1	CHEROKEE SMALL LETTER TLU
					*p = 0x13E1;	//	Ꮱ	0xe1 0x8f 0xa1	CHEROKEE CAPITAL LETTER TLU
					break;
				case 0xABB2:		//	ꮲ	0xea 0xae 0xb2	CHEROKEE SMALL LETTER TLV
					*p = 0x13E2;	//	Ꮲ	0xe1 0x8f 0xa2	CHEROKEE CAPITAL LETTER TLV
					break;
				case 0xABB3:		//	ꮳ	0xea 0xae 0xb3	CHEROKEE SMALL LETTER TSA
					*p = 0x13E3;	//	Ꮳ	0xe1 0x8f 0xa3	CHEROKEE CAPITAL LETTER TSA
					break;
				case 0xABB4:		//	ꮴ	0xea 0xae 0xb4	CHEROKEE SMALL LETTER TSE
					*p = 0x13E4;	//	Ꮴ	0xe1 0x8f 0xa4	CHEROKEE CAPITAL LETTER TSE
					break;
				case 0xABB5:		//	ꮵ	0xea 0xae 0xb5	CHEROKEE SMALL LETTER TSI
					*p = 0x13E5;	//	Ꮵ	0xe1 0x8f 0xa5	CHEROKEE CAPITAL LETTER TSI
					break;
				case 0xABB6:		//	ꮶ	0xea 0xae 0xb6	CHEROKEE SMALL LETTER TSO
					*p = 0x13E6;	//	Ꮶ	0xe1 0x8f 0xa6	CHEROKEE CAPITAL LETTER TSO
					break;
				case 0xABB7:		//	ꮷ	0xea 0xae 0xb7	CHEROKEE SMALL LETTER TSU
					*p = 0x13E7;	//	Ꮷ	0xe1 0x8f 0xa7	CHEROKEE CAPITAL LETTER TSU
					break;
				case 0xABB8:		//	ꮸ	0xea 0xae 0xb8	CHEROKEE SMALL LETTER TSV
					*p = 0x13E8;	//	Ꮸ	0xe1 0x8f 0xa8	CHEROKEE CAPITAL LETTER TSV
					break;
				case 0xABB9:		//	ꮹ	0xea 0xae 0xb9	CHEROKEE SMALL LETTER WA
					*p = 0x13E9;	//	Ꮹ	0xe1 0x8f 0xa9	CHEROKEE CAPITAL LETTER WA
					break;
				case 0xABBA:		//	ꮺ	0xea 0xae 0xba	CHEROKEE SMALL LETTER WE
					*p = 0x13EA;	//	Ꮺ	0xe1 0x8f 0xaa	CHEROKEE CAPITAL LETTER WE
					break;
				case 0xABBB:		//	ꮻ	0xea 0xae 0xbb	CHEROKEE SMALL LETTER WI
					*p = 0x13EB;	//	Ꮻ	0xe1 0x8f 0xab	CHEROKEE CAPITAL LETTER WI
					break;
				case 0xABBC:		//	ꮼ	0xea 0xae 0xbc	CHEROKEE SMALL LETTER WO
					*p = 0x13EC;	//	Ꮼ	0xe1 0x8f 0xac	CHEROKEE CAPITAL LETTER WO
					break;
				case 0xABBD:		//	ꮽ	0xea 0xae 0xbd	CHEROKEE SMALL LETTER WU
					*p = 0x13ED;	//	Ꮽ	0xe1 0x8f 0xad	CHEROKEE CAPITAL LETTER WU
					break;
				case 0xABBE:		//	ꮾ	0xea 0xae 0xbe	CHEROKEE SMALL LETTER WV
					*p = 0x13EE;	//	Ꮾ	0xe1 0x8f 0xae	CHEROKEE CAPITAL LETTER WV
					break;
				case 0xABBF:		//	ꮿ	0xea 0xae 0xbf	CHEROKEE SMALL LETTER YA
					*p = 0x13EF;	//	Ꮿ	0xe1 0x8f 0xaf	CHEROKEE CAPITAL LETTER YA
					break;
				case 0xFF41:		//	ａ	0xef 0xbd 0x81	FULLWIDTH LATIN SMALL LETTER A
					*p = 0xFF21;	//	Ａ	0xef 0xbc 0xa1	FULLWIDTH LATIN CAPITAL LETTER A
					break;
				case 0xFF42:		//	ｂ	0xef 0xbd 0x82	FULLWIDTH LATIN SMALL LETTER B
					*p = 0xFF22;	//	Ｂ	0xef 0xbc 0xa2	FULLWIDTH LATIN CAPITAL LETTER B
					break;
				case 0xFF43:		//	ｃ	0xef 0xbd 0x83	FULLWIDTH LATIN SMALL LETTER C
					*p = 0xFF23;	//	Ｃ	0xef 0xbc 0xa3	FULLWIDTH LATIN CAPITAL LETTER C
					break;
				case 0xFF44:		//	ｄ	0xef 0xbd 0x84	FULLWIDTH LATIN SMALL LETTER D
					*p = 0xFF24;	//	Ｄ	0xef 0xbc 0xa4	FULLWIDTH LATIN CAPITAL LETTER D
					break;
				case 0xFF45:		//	ｅ	0xef 0xbd 0x85	FULLWIDTH LATIN SMALL LETTER E
					*p = 0xFF25;	//	Ｅ	0xef 0xbc 0xa5	FULLWIDTH LATIN CAPITAL LETTER E
					break;
				case 0xFF46:		//	ｆ	0xef 0xbd 0x86	FULLWIDTH LATIN SMALL LETTER F
					*p = 0xFF26;	//	Ｆ	0xef 0xbc 0xa6	FULLWIDTH LATIN CAPITAL LETTER F
					break;
				case 0xFF47:		//	ｇ	0xef 0xbd 0x87	FULLWIDTH LATIN SMALL LETTER G
					*p = 0xFF27;	//	Ｇ	0xef 0xbc 0xa7	FULLWIDTH LATIN CAPITAL LETTER G
					break;
				case 0xFF48:		//	ｈ	0xef 0xbd 0x88	FULLWIDTH LATIN SMALL LETTER H
					*p = 0xFF28;	//	Ｈ	0xef 0xbc 0xa8	FULLWIDTH LATIN CAPITAL LETTER H
					break;
				case 0xFF49:		//	ｉ	0xef 0xbd 0x89	FULLWIDTH LATIN SMALL LETTER I
					*p = 0xFF29;	//	Ｉ	0xef 0xbc 0xa9	FULLWIDTH LATIN CAPITAL LETTER I
					break;
				case 0xFF4A:		//	ｊ	0xef 0xbd 0x8a	FULLWIDTH LATIN SMALL LETTER J
					*p = 0xFF2A;	//	Ｊ	0xef 0xbc 0xaa	FULLWIDTH LATIN CAPITAL LETTER J
					break;
				case 0xFF4B:		//	ｋ	0xef 0xbd 0x8b	FULLWIDTH LATIN SMALL LETTER K
					*p = 0xFF2B;	//	Ｋ	0xef 0xbc 0xab	FULLWIDTH LATIN CAPITAL LETTER K
					break;
				case 0xFF4C:		//	ｌ	0xef 0xbd 0x8c	FULLWIDTH LATIN SMALL LETTER L
					*p = 0xFF2C;	//	Ｌ	0xef 0xbc 0xac	FULLWIDTH LATIN CAPITAL LETTER L
					break;
				case 0xFF4D:		//	ｍ	0xef 0xbd 0x8d	FULLWIDTH LATIN SMALL LETTER M
					*p = 0xFF2D;	//	Ｍ	0xef 0xbc 0xad	FULLWIDTH LATIN CAPITAL LETTER M
					break;
				case 0xFF4E:		//	ｎ	0xef 0xbd 0x8e	FULLWIDTH LATIN SMALL LETTER N
					*p = 0xFF2E;	//	Ｎ	0xef 0xbc 0xae	FULLWIDTH LATIN CAPITAL LETTER N
					break;
				case 0xFF4F:		//	ｏ	0xef 0xbd 0x8f	FULLWIDTH LATIN SMALL LETTER O
					*p = 0xFF2F;	//	Ｏ	0xef 0xbc 0xaf	FULLWIDTH LATIN CAPITAL LETTER O
					break;
				case 0xFF50:		//	ｐ	0xef 0xbd 0x90	FULLWIDTH LATIN SMALL LETTER P
					*p = 0xFF30;	//	Ｐ	0xef 0xbc 0xb0	FULLWIDTH LATIN CAPITAL LETTER P
					break;
				case 0xFF51:		//	ｑ	0xef 0xbd 0x91	FULLWIDTH LATIN SMALL LETTER Q
					*p = 0xFF31;	//	Ｑ	0xef 0xbc 0xb1	FULLWIDTH LATIN CAPITAL LETTER Q
					break;
				case 0xFF52:		//	ｒ	0xef 0xbd 0x92	FULLWIDTH LATIN SMALL LETTER R
					*p = 0xFF32;	//	Ｒ	0xef 0xbc 0xb2	FULLWIDTH LATIN CAPITAL LETTER R
					break;
				case 0xFF53:		//	ｓ	0xef 0xbd 0x93	FULLWIDTH LATIN SMALL LETTER S
					*p = 0xFF33;	//	Ｓ	0xef 0xbc 0xb3	FULLWIDTH LATIN CAPITAL LETTER S
					break;
				case 0xFF54:		//	ｔ	0xef 0xbd 0x94	FULLWIDTH LATIN SMALL LETTER T
					*p = 0xFF34;	//	Ｔ	0xef 0xbc 0xb4	FULLWIDTH LATIN CAPITAL LETTER T
					break;
				case 0xFF55:		//	ｕ	0xef 0xbd 0x95	FULLWIDTH LATIN SMALL LETTER U
					*p = 0xFF35;	//	Ｕ	0xef 0xbc 0xb5	FULLWIDTH LATIN CAPITAL LETTER U
					break;
				case 0xFF56:		//	ｖ	0xef 0xbd 0x96	FULLWIDTH LATIN SMALL LETTER V
					*p = 0xFF36;	//	Ｖ	0xef 0xbc 0xb6	FULLWIDTH LATIN CAPITAL LETTER V
					break;
				case 0xFF57:		//	ｗ	0xef 0xbd 0x97	FULLWIDTH LATIN SMALL LETTER W
					*p = 0xFF37;	//	Ｗ	0xef 0xbc 0xb7	FULLWIDTH LATIN CAPITAL LETTER W
					break;
				case 0xFF58:		//	ｘ	0xef 0xbd 0x98	FULLWIDTH LATIN SMALL LETTER X
					*p = 0xFF38;	//	Ｘ	0xef 0xbc 0xb8	FULLWIDTH LATIN CAPITAL LETTER X
					break;
				case 0xFF59:		//	ｙ	0xef 0xbd 0x99	FULLWIDTH LATIN SMALL LETTER Y
					*p = 0xFF39;	//	Ｙ	0xef 0xbc 0xb9	FULLWIDTH LATIN CAPITAL LETTER Y
					break;
				case 0xFF5A:		//	ｚ	0xef 0xbd 0x9a	FULLWIDTH LATIN SMALL LETTER Z
					*p = 0xFF3A;	//	Ｚ	0xef 0xbc 0xba	FULLWIDTH LATIN CAPITAL LETTER Z
					break;
				case 0x10428:		//	𐐨	0xf0 0x90 0x90 0xa8	DESERET SMALL LETTER LONG I
					*p = 0x10400;	//	𐐀	0xf0 0x90 0x90 0x80	DESERET CAPITAL LETTER LONG I
					break;
				case 0x10429:		//	𐐩	0xf0 0x90 0x90 0xa9	DESERET SMALL LETTER LONG E
					*p = 0x10401;	//	𐐁	0xf0 0x90 0x90 0x81	DESERET CAPITAL LETTER LONG E
					break;
				case 0x1042A:		//	𐐪	0xf0 0x90 0x90 0xaa	DESERET SMALL LETTER LONG A
					*p = 0x10402;	//	𐐂	0xf0 0x90 0x90 0x82	DESERET CAPITAL LETTER LONG A
					break;
				case 0x1042B:		//	𐐫	0xf0 0x90 0x90 0xab	DESERET SMALL LETTER LONG AH
					*p = 0x10403;	//	𐐃	0xf0 0x90 0x90 0x83	DESERET CAPITAL LETTER LONG AH
					break;
				case 0x1042C:		//	𐐬	0xf0 0x90 0x90 0xac	DESERET SMALL LETTER LONG O
					*p = 0x10404;	//	𐐄	0xf0 0x90 0x90 0x84	DESERET CAPITAL LETTER LONG O
					break;
				case 0x1042D:		//	𐐭	0xf0 0x90 0x90 0xad	DESERET SMALL LETTER LONG OO
					*p = 0x10405;	//	𐐅	0xf0 0x90 0x90 0x85	DESERET CAPITAL LETTER LONG OO
					break;
				case 0x1042E:		//	𐐮	0xf0 0x90 0x90 0xae	DESERET SMALL LETTER SHORT I
					*p = 0x10406;	//	𐐆	0xf0 0x90 0x90 0x86	DESERET CAPITAL LETTER SHORT I
					break;
				case 0x1042F:		//	𐐯	0xf0 0x90 0x90 0xaf	DESERET SMALL LETTER SHORT E
					*p = 0x10407;	//	𐐇	0xf0 0x90 0x90 0x87	DESERET CAPITAL LETTER SHORT E
					break;
				case 0x10430:		//	𐐰	0xf0 0x90 0x90 0xb0	DESERET SMALL LETTER SHORT A
					*p = 0x10408;	//	𐐈	0xf0 0x90 0x90 0x88	DESERET CAPITAL LETTER SHORT A
					break;
				case 0x10431:		//	𐐱	0xf0 0x90 0x90 0xb1	DESERET SMALL LETTER SHORT AH
					*p = 0x10409;	//	𐐉	0xf0 0x90 0x90 0x89	DESERET CAPITAL LETTER SHORT AH
					break;
				case 0x10432:		//	𐐲	0xf0 0x90 0x90 0xb2	DESERET SMALL LETTER SHORT O
					*p = 0x1040A;	//	𐐊	0xf0 0x90 0x90 0x8a	DESERET CAPITAL LETTER SHORT O
					break;
				case 0x10433:		//	𐐳	0xf0 0x90 0x90 0xb3	DESERET SMALL LETTER SHORT OO
					*p = 0x1040B;	//	𐐋	0xf0 0x90 0x90 0x8b	DESERET CAPITAL LETTER SHORT OO
					break;
				case 0x10434:		//	𐐴	0xf0 0x90 0x90 0xb4	DESERET SMALL LETTER AY
					*p = 0x1040C;	//	𐐌	0xf0 0x90 0x90 0x8c	DESERET CAPITAL LETTER AY
					break;
				case 0x10435:		//	𐐵	0xf0 0x90 0x90 0xb5	DESERET SMALL LETTER OW
					*p = 0x1040D;	//	𐐍	0xf0 0x90 0x90 0x8d	DESERET CAPITAL LETTER OW
					break;
				case 0x10436:		//	𐐶	0xf0 0x90 0x90 0xb6	DESERET SMALL LETTER WU
					*p = 0x1040E;	//	𐐎	0xf0 0x90 0x90 0x8e	DESERET CAPITAL LETTER WU
					break;
				case 0x10437:		//	𐐷	0xf0 0x90 0x90 0xb7	DESERET SMALL LETTER YEE
					*p = 0x1040F;	//	𐐏	0xf0 0x90 0x90 0x8f	DESERET CAPITAL LETTER YEE
					break;
				case 0x10438:		//	𐐸	0xf0 0x90 0x90 0xb8	DESERET SMALL LETTER H
					*p = 0x10410;	//	𐐐	0xf0 0x90 0x90 0x90	DESERET CAPITAL LETTER H
					break;
				case 0x10439:		//	𐐹	0xf0 0x90 0x90 0xb9	DESERET SMALL LETTER PEE
					*p = 0x10411;	//	𐐑	0xf0 0x90 0x90 0x91	DESERET CAPITAL LETTER PEE
					break;
				case 0x1043A:		//	𐐺	0xf0 0x90 0x90 0xba	DESERET SMALL LETTER BEE
					*p = 0x10412;	//	𐐒	0xf0 0x90 0x90 0x92	DESERET CAPITAL LETTER BEE
					break;
				case 0x1043B:		//	𐐻	0xf0 0x90 0x90 0xbb	DESERET SMALL LETTER TEE
					*p = 0x10413;	//	𐐓	0xf0 0x90 0x90 0x93	DESERET CAPITAL LETTER TEE
					break;
				case 0x1043C:		//	𐐼	0xf0 0x90 0x90 0xbc	DESERET SMALL LETTER DEE
					*p = 0x10414;	//	𐐔	0xf0 0x90 0x90 0x94	DESERET CAPITAL LETTER DEE
					break;
				case 0x1043D:		//	𐐽	0xf0 0x90 0x90 0xbd	DESERET SMALL LETTER CHEE
					*p = 0x10415;	//	𐐕	0xf0 0x90 0x90 0x95	DESERET CAPITAL LETTER CHEE
					break;
				case 0x1043E:		//	𐐾	0xf0 0x90 0x90 0xbe	DESERET SMALL LETTER JEE
					*p = 0x10416;	//	𐐖	0xf0 0x90 0x90 0x96	DESERET CAPITAL LETTER JEE
					break;
				case 0x1043F:		//	𐐿	0xf0 0x90 0x90 0xbf	DESERET SMALL LETTER KAY
					*p = 0x10417;	//	𐐗	0xf0 0x90 0x90 0x97	DESERET CAPITAL LETTER KAY
					break;
				case 0x10440:		//	𐑀	0xf0 0x90 0x91 0x80	DESERET SMALL LETTER GAY
					*p = 0x10418;	//	𐐘	0xf0 0x90 0x90 0x98	DESERET CAPITAL LETTER GAY
					break;
				case 0x10441:		//	𐑁	0xf0 0x90 0x91 0x81	DESERET SMALL LETTER EF
					*p = 0x10419;	//	𐐙	0xf0 0x90 0x90 0x99	DESERET CAPITAL LETTER EF
					break;
				case 0x10442:		//	𐑂	0xf0 0x90 0x91 0x82	DESERET SMALL LETTER VEE
					*p = 0x1041A;	//	𐐚	0xf0 0x90 0x90 0x9a	DESERET CAPITAL LETTER VEE
					break;
				case 0x10443:		//	𐑃	0xf0 0x90 0x91 0x83	DESERET SMALL LETTER ETH
					*p = 0x1041B;	//	𐐛	0xf0 0x90 0x90 0x9b	DESERET CAPITAL LETTER ETH
					break;
				case 0x10444:		//	𐑄	0xf0 0x90 0x91 0x84	DESERET SMALL LETTER THEE
					*p = 0x1041C;	//	𐐜	0xf0 0x90 0x90 0x9c	DESERET CAPITAL LETTER THEE
					break;
				case 0x10445:		//	𐑅	0xf0 0x90 0x91 0x85	DESERET SMALL LETTER ES
					*p = 0x1041D;	//	𐐝	0xf0 0x90 0x90 0x9d	DESERET CAPITAL LETTER ES
					break;
				case 0x10446:		//	𐑆	0xf0 0x90 0x91 0x86	DESERET SMALL LETTER ZEE
					*p = 0x1041E;	//	𐐞	0xf0 0x90 0x90 0x9e	DESERET CAPITAL LETTER ZEE
					break;
				case 0x10447:		//	𐑇	0xf0 0x90 0x91 0x87	DESERET SMALL LETTER ESH
					*p = 0x1041F;	//	𐐟	0xf0 0x90 0x90 0x9f	DESERET CAPITAL LETTER ESH
					break;
				case 0x10448:		//	𐑈	0xf0 0x90 0x91 0x88	DESERET SMALL LETTER ZHEE
					*p = 0x10420;	//	𐐠	0xf0 0x90 0x90 0xa0	DESERET CAPITAL LETTER ZHEE
					break;
				case 0x10449:		//	𐑉	0xf0 0x90 0x91 0x89	DESERET SMALL LETTER ER
					*p = 0x10421;	//	𐐡	0xf0 0x90 0x90 0xa1	DESERET CAPITAL LETTER ER
					break;
				case 0x1044A:		//	𐑊	0xf0 0x90 0x91 0x8a	DESERET SMALL LETTER EL
					*p = 0x10422;	//	𐐢	0xf0 0x90 0x90 0xa2	DESERET CAPITAL LETTER EL
					break;
				case 0x1044B:		//	𐑋	0xf0 0x90 0x91 0x8b	DESERET SMALL LETTER EM
					*p = 0x10423;	//	𐐣	0xf0 0x90 0x90 0xa3	DESERET CAPITAL LETTER EM
					break;
				case 0x1044C:		//	𐑌	0xf0 0x90 0x91 0x8c	DESERET SMALL LETTER EN
					*p = 0x10424;	//	𐐤	0xf0 0x90 0x90 0xa4	DESERET CAPITAL LETTER EN
					break;
				case 0x1044D:		//	𐑍	0xf0 0x90 0x91 0x8d	DESERET SMALL LETTER ENG
					*p = 0x10425;	//	𐐥	0xf0 0x90 0x90 0xa5	DESERET CAPITAL LETTER ENG
					break;
				case 0x1044E:		//	𐑎	0xf0 0x90 0x91 0x8e	DESERET SMALL LETTER OI
					*p = 0x10426;	//	𐐦	0xf0 0x90 0x90 0xa6	DESERET CAPITAL LETTER OI
					break;
				case 0x1044F:		//	𐑏	0xf0 0x90 0x91 0x8f	DESERET SMALL LETTER EW
					*p = 0x10427;	//	𐐧	0xf0 0x90 0x90 0xa7	DESERET CAPITAL LETTER EW
					break;
				case 0x104D8:		//	𐓘	0xf0 0x90 0x93 0x98	OSAGE SMALL LETTER A
					*p = 0x104B0;	//	𐒰	0xf0 0x90 0x92 0xb0	OSAGE CAPITAL LETTER A
					break;
				case 0x104D9:		//	𐓙	0xf0 0x90 0x93 0x99	OSAGE SMALL LETTER AI
					*p = 0x104B1;	//	𐒱	0xf0 0x90 0x92 0xb1	OSAGE CAPITAL LETTER AI
					break;
				case 0x104DA:		//	𐓚	0xf0 0x90 0x93 0x9a	OSAGE SMALL LETTER AIN
					*p = 0x104B2;	//	𐒲	0xf0 0x90 0x92 0xb2	OSAGE CAPITAL LETTER AIN
					break;
				case 0x104DB:		//	𐓛	0xf0 0x90 0x93 0x9b	OSAGE SMALL LETTER AH
					*p = 0x104B3;	//	𐒳	0xf0 0x90 0x92 0xb3	OSAGE CAPITAL LETTER AH
					break;
				case 0x104DC:		//	𐓜	0xf0 0x90 0x93 0x9c	OSAGE SMALL LETTER BRA
					*p = 0x104B4;	//	𐒴	0xf0 0x90 0x92 0xb4	OSAGE CAPITAL LETTER BRA
					break;
				case 0x104DD:		//	𐓝	0xf0 0x90 0x93 0x9d	OSAGE SMALL LETTER CHA
					*p = 0x104B5;	//	𐒵	0xf0 0x90 0x92 0xb5	OSAGE CAPITAL LETTER CHA
					break;
				case 0x104DE:		//	𐓞	0xf0 0x90 0x93 0x9e	OSAGE SMALL LETTER EHCHA
					*p = 0x104B6;	//	𐒶	0xf0 0x90 0x92 0xb6	OSAGE CAPITAL LETTER EHCHA
					break;
				case 0x104DF:		//	𐓟	0xf0 0x90 0x93 0x9f	OSAGE SMALL LETTER E
					*p = 0x104B7;	//	𐒷	0xf0 0x90 0x92 0xb7	OSAGE CAPITAL LETTER E
					break;
				case 0x104E0:		//	𐓠	0xf0 0x90 0x93 0xa0	OSAGE SMALL LETTER EIN
					*p = 0x104B8;	//	𐒸	0xf0 0x90 0x92 0xb8	OSAGE CAPITAL LETTER EIN
					break;
				case 0x104E1:		//	𐓡	0xf0 0x90 0x93 0xa1	OSAGE SMALL LETTER HA
					*p = 0x104B9;	//	𐒹	0xf0 0x90 0x92 0xb9	OSAGE CAPITAL LETTER HA
					break;
				case 0x104E2:		//	𐓢	0xf0 0x90 0x93 0xa2	OSAGE SMALL LETTER HYA
					*p = 0x104BA;	//	𐒺	0xf0 0x90 0x92 0xba	OSAGE CAPITAL LETTER HYA
					break;
				case 0x104E3:		//	𐓣	0xf0 0x90 0x93 0xa3	OSAGE SMALL LETTER I
					*p = 0x104BB;	//	𐒻	0xf0 0x90 0x92 0xbb	OSAGE CAPITAL LETTER I
					break;
				case 0x104E4:		//	𐓤	0xf0 0x90 0x93 0xa4	OSAGE SMALL LETTER KA
					*p = 0x104BC;	//	𐒼	0xf0 0x90 0x92 0xbc	OSAGE CAPITAL LETTER KA
					break;
				case 0x104E5:		//	𐓥	0xf0 0x90 0x93 0xa5	OSAGE SMALL LETTER EHKA
					*p = 0x104BD;	//	𐒽	0xf0 0x90 0x92 0xbd	OSAGE CAPITAL LETTER EHKA
					break;
				case 0x104E6:		//	𐓦	0xf0 0x90 0x93 0xa6	OSAGE SMALL LETTER KYA
					*p = 0x104BE;	//	𐒾	0xf0 0x90 0x92 0xbe	OSAGE CAPITAL LETTER KYA
					break;
				case 0x104E7:		//	𐓧	0xf0 0x90 0x93 0xa7	OSAGE SMALL LETTER LA
					*p = 0x104BF;	//	𐒿	0xf0 0x90 0x92 0xbf	OSAGE CAPITAL LETTER LA
					break;
				case 0x104E8:		//	𐓨	0xf0 0x90 0x93 0xa8	OSAGE SMALL LETTER MA
					*p = 0x104C0;	//	𐓀	0xf0 0x90 0x93 0x80	OSAGE CAPITAL LETTER MA
					break;
				case 0x104E9:		//	𐓩	0xf0 0x90 0x93 0xa9	OSAGE SMALL LETTER NA
					*p = 0x104C1;	//	𐓁	0xf0 0x90 0x93 0x81	OSAGE CAPITAL LETTER NA
					break;
				case 0x104EA:		//	𐓪	0xf0 0x90 0x93 0xaa	OSAGE SMALL LETTER O
					*p = 0x104C2;	//	𐓂	0xf0 0x90 0x93 0x82	OSAGE CAPITAL LETTER O
					break;
				case 0x104EB:		//	𐓫	0xf0 0x90 0x93 0xab	OSAGE SMALL LETTER OIN
					*p = 0x104C3;	//	𐓃	0xf0 0x90 0x93 0x83	OSAGE CAPITAL LETTER OIN
					break;
				case 0x104EC:		//	𐓬	0xf0 0x90 0x93 0xac	OSAGE SMALL LETTER PA
					*p = 0x104C4;	//	𐓄	0xf0 0x90 0x93 0x84	OSAGE CAPITAL LETTER PA
					break;
				case 0x104ED:		//	𐓭	0xf0 0x90 0x93 0xad	OSAGE SMALL LETTER EHPA
					*p = 0x104C5;	//	𐓅	0xf0 0x90 0x93 0x85	OSAGE CAPITAL LETTER EHPA
					break;
				case 0x104EE:		//	𐓮	0xf0 0x90 0x93 0xae	OSAGE SMALL LETTER SA
					*p = 0x104C6;	//	𐓆	0xf0 0x90 0x93 0x86	OSAGE CAPITAL LETTER SA
					break;
				case 0x104EF:		//	𐓯	0xf0 0x90 0x93 0xaf	OSAGE SMALL LETTER SHA
					*p = 0x104C7;	//	𐓇	0xf0 0x90 0x93 0x87	OSAGE CAPITAL LETTER SHA
					break;
				case 0x104F0:		//	𐓰	0xf0 0x90 0x93 0xb0	OSAGE SMALL LETTER TA
					*p = 0x104C8;	//	𐓈	0xf0 0x90 0x93 0x88	OSAGE CAPITAL LETTER TA
					break;
				case 0x104F1:		//	𐓱	0xf0 0x90 0x93 0xb1	OSAGE SMALL LETTER EHTA
					*p = 0x104C9;	//	𐓉	0xf0 0x90 0x93 0x89	OSAGE CAPITAL LETTER EHTA
					break;
				case 0x104F2:		//	𐓲	0xf0 0x90 0x93 0xb2	OSAGE SMALL LETTER TSA
					*p = 0x104CA;	//	𐓊	0xf0 0x90 0x93 0x8a	OSAGE CAPITAL LETTER TSA
					break;
				case 0x104F3:		//	𐓳	0xf0 0x90 0x93 0xb3	OSAGE SMALL LETTER EHTSA
					*p = 0x104CB;	//	𐓋	0xf0 0x90 0x93 0x8b	OSAGE CAPITAL LETTER EHTSA
					break;
				case 0x104F4:		//	𐓴	0xf0 0x90 0x93 0xb4	OSAGE SMALL LETTER TSHA
					*p = 0x104CC;	//	𐓌	0xf0 0x90 0x93 0x8c	OSAGE CAPITAL LETTER TSHA
					break;
				case 0x104F5:		//	𐓵	0xf0 0x90 0x93 0xb5	OSAGE SMALL LETTER DHA
					*p = 0x104CD;	//	𐓍	0xf0 0x90 0x93 0x8d	OSAGE CAPITAL LETTER DHA
					break;
				case 0x104F6:		//	𐓶	0xf0 0x90 0x93 0xb6	OSAGE SMALL LETTER U
					*p = 0x104CE;	//	𐓎	0xf0 0x90 0x93 0x8e	OSAGE CAPITAL LETTER U
					break;
				case 0x104F7:		//	𐓷	0xf0 0x90 0x93 0xb7	OSAGE SMALL LETTER WA
					*p = 0x104CF;	//	𐓏	0xf0 0x90 0x93 0x8f	OSAGE CAPITAL LETTER WA
					break;
				case 0x104F8:		//	𐓸	0xf0 0x90 0x93 0xb8	OSAGE SMALL LETTER KHA
					*p = 0x104D0;	//	𐓐	0xf0 0x90 0x93 0x90	OSAGE CAPITAL LETTER KHA
					break;
				case 0x104F9:		//	𐓹	0xf0 0x90 0x93 0xb9	OSAGE SMALL LETTER GHA
					*p = 0x104D1;	//	𐓑	0xf0 0x90 0x93 0x91	OSAGE CAPITAL LETTER GHA
					break;
				case 0x104FA:		//	𐓺	0xf0 0x90 0x93 0xba	OSAGE SMALL LETTER ZA
					*p = 0x104D2;	//	𐓒	0xf0 0x90 0x93 0x92	OSAGE CAPITAL LETTER ZA
					break;
				case 0x104FB:		//	𐓻	0xf0 0x90 0x93 0xbb	OSAGE SMALL LETTER ZHA
					*p = 0x104D3;	//	𐓓	0xf0 0x90 0x93 0x93	OSAGE CAPITAL LETTER ZHA
					break;
				case 0x10CC0:		//	𐳀	0xf0 0x90 0xb3 0x80	OLD HUNGARIAN SMALL LETTER A
					*p = 0x10C80;	//	𐲀	0xf0 0x90 0xb2 0x80	OLD HUNGARIAN CAPITAL LETTER A
					break;
				case 0x10CC1:		//	𐳁	0xf0 0x90 0xb3 0x81	OLD HUNGARIAN SMALL LETTER AA
					*p = 0x10C81;	//	𐲁	0xf0 0x90 0xb2 0x81	OLD HUNGARIAN CAPITAL LETTER AA
					break;
				case 0x10CC2:		//	𐳂	0xf0 0x90 0xb3 0x82	OLD HUNGARIAN SMALL LETTER EB
					*p = 0x10C82;	//	𐲂	0xf0 0x90 0xb2 0x82	OLD HUNGARIAN CAPITAL LETTER EB
					break;
				case 0x10CC3:		//	𐳃	0xf0 0x90 0xb3 0x83	OLD HUNGARIAN SMALL LETTER AMB
					*p = 0x10C83;	//	𐲃	0xf0 0x90 0xb2 0x83	OLD HUNGARIAN CAPITAL LETTER AMB
					break;
				case 0x10CC4:		//	𐳄	0xf0 0x90 0xb3 0x84	OLD HUNGARIAN SMALL LETTER EC
					*p = 0x10C84;	//	𐲄	0xf0 0x90 0xb2 0x84	OLD HUNGARIAN CAPITAL LETTER EC
					break;
				case 0x10CC5:		//	𐳅	0xf0 0x90 0xb3 0x85	OLD HUNGARIAN SMALL LETTER ENC
					*p = 0x10C85;	//	𐲅	0xf0 0x90 0xb2 0x85	OLD HUNGARIAN CAPITAL LETTER ENC
					break;
				case 0x10CC6:		//	𐳆	0xf0 0x90 0xb3 0x86	OLD HUNGARIAN SMALL LETTER ECS
					*p = 0x10C86;	//	𐲆	0xf0 0x90 0xb2 0x86	OLD HUNGARIAN CAPITAL LETTER ECS
					break;
				case 0x10CC7:		//	𐳇	0xf0 0x90 0xb3 0x87	OLD HUNGARIAN SMALL LETTER ED
					*p = 0x10C87;	//	𐲇	0xf0 0x90 0xb2 0x87	OLD HUNGARIAN CAPITAL LETTER ED
					break;
				case 0x10CC8:		//	𐳈	0xf0 0x90 0xb3 0x88	OLD HUNGARIAN SMALL LETTER AND
					*p = 0x10C88;	//	𐲈	0xf0 0x90 0xb2 0x88	OLD HUNGARIAN CAPITAL LETTER AND
					break;
				case 0x10CC9:		//	𐳉	0xf0 0x90 0xb3 0x89	OLD HUNGARIAN SMALL LETTER E
					*p = 0x10C89;	//	𐲉	0xf0 0x90 0xb2 0x89	OLD HUNGARIAN CAPITAL LETTER E
					break;
				case 0x10CCA:		//	𐳊	0xf0 0x90 0xb3 0x8a	OLD HUNGARIAN SMALL LETTER CLOSE E
					*p = 0x10C8A;	//	𐲊	0xf0 0x90 0xb2 0x8a	OLD HUNGARIAN CAPITAL LETTER CLOSE E
					break;
				case 0x10CCB:		//	𐳋	0xf0 0x90 0xb3 0x8b	OLD HUNGARIAN SMALL LETTER EE
					*p = 0x10C8B;	//	𐲋	0xf0 0x90 0xb2 0x8b	OLD HUNGARIAN CAPITAL LETTER EE
					break;
				case 0x10CCC:		//	𐳌	0xf0 0x90 0xb3 0x8c	OLD HUNGARIAN SMALL LETTER EF
					*p = 0x10C8C;	//	𐲌	0xf0 0x90 0xb2 0x8c	OLD HUNGARIAN CAPITAL LETTER EF
					break;
				case 0x10CCD:		//	𐳍	0xf0 0x90 0xb3 0x8d	OLD HUNGARIAN SMALL LETTER EG
					*p = 0x10C8D;	//	𐲍	0xf0 0x90 0xb2 0x8d	OLD HUNGARIAN CAPITAL LETTER EG
					break;
				case 0x10CCE:		//	𐳎	0xf0 0x90 0xb3 0x8e	OLD HUNGARIAN SMALL LETTER EGY
					*p = 0x10C8E;	//	𐲎	0xf0 0x90 0xb2 0x8e	OLD HUNGARIAN CAPITAL LETTER EGY
					break;
				case 0x10CCF:		//	𐳏	0xf0 0x90 0xb3 0x8f	OLD HUNGARIAN SMALL LETTER EH
					*p = 0x10C8F;	//	𐲏	0xf0 0x90 0xb2 0x8f	OLD HUNGARIAN CAPITAL LETTER EH
					break;
				case 0x10CD0:		//	𐳐	0xf0 0x90 0xb3 0x90	OLD HUNGARIAN SMALL LETTER I
					*p = 0x10C90;	//	𐲐	0xf0 0x90 0xb2 0x90	OLD HUNGARIAN CAPITAL LETTER I
					break;
				case 0x10CD1:		//	𐳑	0xf0 0x90 0xb3 0x91	OLD HUNGARIAN SMALL LETTER II
					*p = 0x10C91;	//	𐲑	0xf0 0x90 0xb2 0x91	OLD HUNGARIAN CAPITAL LETTER II
					break;
				case 0x10CD2:		//	𐳒	0xf0 0x90 0xb3 0x92	OLD HUNGARIAN SMALL LETTER EJ
					*p = 0x10C92;	//	𐲒	0xf0 0x90 0xb2 0x92	OLD HUNGARIAN CAPITAL LETTER EJ
					break;
				case 0x10CD3:		//	𐳓	0xf0 0x90 0xb3 0x93	OLD HUNGARIAN SMALL LETTER EK
					*p = 0x10C93;	//	𐲓	0xf0 0x90 0xb2 0x93	OLD HUNGARIAN CAPITAL LETTER EK
					break;
				case 0x10CD4:		//	𐳔	0xf0 0x90 0xb3 0x94	OLD HUNGARIAN SMALL LETTER AK
					*p = 0x10C94;	//	𐲔	0xf0 0x90 0xb2 0x94	OLD HUNGARIAN CAPITAL LETTER AK
					break;
				case 0x10CD5:		//	𐳕	0xf0 0x90 0xb3 0x95	OLD HUNGARIAN SMALL LETTER UNK
					*p = 0x10C95;	//	𐲕	0xf0 0x90 0xb2 0x95	OLD HUNGARIAN CAPITAL LETTER UNK
					break;
				case 0x10CD6:		//	𐳖	0xf0 0x90 0xb3 0x96	OLD HUNGARIAN SMALL LETTER EL
					*p = 0x10C96;	//	𐲖	0xf0 0x90 0xb2 0x96	OLD HUNGARIAN CAPITAL LETTER EL
					break;
				case 0x10CD7:		//	𐳗	0xf0 0x90 0xb3 0x97	OLD HUNGARIAN SMALL LETTER ELY
					*p = 0x10C97;	//	𐲗	0xf0 0x90 0xb2 0x97	OLD HUNGARIAN CAPITAL LETTER ELY
					break;
				case 0x10CD8:		//	𐳘	0xf0 0x90 0xb3 0x98	OLD HUNGARIAN SMALL LETTER EM
					*p = 0x10C98;	//	𐲘	0xf0 0x90 0xb2 0x98	OLD HUNGARIAN CAPITAL LETTER EM
					break;
				case 0x10CD9:		//	𐳙	0xf0 0x90 0xb3 0x99	OLD HUNGARIAN SMALL LETTER EN
					*p = 0x10C99;	//	𐲙	0xf0 0x90 0xb2 0x99	OLD HUNGARIAN CAPITAL LETTER EN
					break;
				case 0x10CDA:		//	𐳚	0xf0 0x90 0xb3 0x9a	OLD HUNGARIAN SMALL LETTER ENY
					*p = 0x10C9A;	//	𐲚	0xf0 0x90 0xb2 0x9a	OLD HUNGARIAN CAPITAL LETTER ENY
					break;
				case 0x10CDB:		//	𐳛	0xf0 0x90 0xb3 0x9b	OLD HUNGARIAN SMALL LETTER O
					*p = 0x10C9B;	//	𐲛	0xf0 0x90 0xb2 0x9b	OLD HUNGARIAN CAPITAL LETTER O
					break;
				case 0x10CDC:		//	𐳜	0xf0 0x90 0xb3 0x9c	OLD HUNGARIAN SMALL LETTER OO
					*p = 0x10C9C;	//	𐲜	0xf0 0x90 0xb2 0x9c	OLD HUNGARIAN CAPITAL LETTER OO
					break;
				case 0x10CDD:		//	𐳝	0xf0 0x90 0xb3 0x9d	OLD HUNGARIAN SMALL LETTER NIKOLSBURG OE
					*p = 0x10C9D;	//	𐲝	0xf0 0x90 0xb2 0x9d	OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG OE
					break;
				case 0x10CDE:		//	𐳞	0xf0 0x90 0xb3 0x9e	OLD HUNGARIAN SMALL LETTER RUDIMENTA OE
					*p = 0x10C9E;	//	𐲞	0xf0 0x90 0xb2 0x9e	OLD HUNGARIAN CAPITAL LETTER RUDIMENTA OE
					break;
				case 0x10CDF:		//	𐳟	0xf0 0x90 0xb3 0x9f	OLD HUNGARIAN SMALL LETTER OEE
					*p = 0x10C9F;	//	𐲟	0xf0 0x90 0xb2 0x9f	OLD HUNGARIAN CAPITAL LETTER OEE
					break;
				case 0x10CE0:		//	𐳠	0xf0 0x90 0xb3 0xa0	OLD HUNGARIAN SMALL LETTER EP
					*p = 0x10CA0;	//	𐲠	0xf0 0x90 0xb2 0xa0	OLD HUNGARIAN CAPITAL LETTER EP
					break;
				case 0x10CE1:		//	𐳡	0xf0 0x90 0xb3 0xa1	OLD HUNGARIAN SMALL LETTER EMP
					*p = 0x10CA1;	//	𐲡	0xf0 0x90 0xb2 0xa1	OLD HUNGARIAN CAPITAL LETTER EMP
					break;
				case 0x10CE2:		//	𐳢	0xf0 0x90 0xb3 0xa2	OLD HUNGARIAN SMALL LETTER ER
					*p = 0x10CA2;	//	𐲢	0xf0 0x90 0xb2 0xa2	OLD HUNGARIAN CAPITAL LETTER ER
					break;
				case 0x10CE3:		//	𐳣	0xf0 0x90 0xb3 0xa3	OLD HUNGARIAN SMALL LETTER SHORT ER
					*p = 0x10CA3;	//	𐲣	0xf0 0x90 0xb2 0xa3	OLD HUNGARIAN CAPITAL LETTER SHORT ER
					break;
				case 0x10CE4:		//	𐳤	0xf0 0x90 0xb3 0xa4	OLD HUNGARIAN SMALL LETTER ES
					*p = 0x10CA4;	//	𐲤	0xf0 0x90 0xb2 0xa4	OLD HUNGARIAN CAPITAL LETTER ES
					break;
				case 0x10CE5:		//	𐳥	0xf0 0x90 0xb3 0xa5	OLD HUNGARIAN SMALL LETTER ESZ
					*p = 0x10CA5;	//	𐲥	0xf0 0x90 0xb2 0xa5	OLD HUNGARIAN CAPITAL LETTER ESZ
					break;
				case 0x10CE6:		//	𐳦	0xf0 0x90 0xb3 0xa6	OLD HUNGARIAN SMALL LETTER ET
					*p = 0x10CA6;	//	𐲦	0xf0 0x90 0xb2 0xa6	OLD HUNGARIAN CAPITAL LETTER ET
					break;
				case 0x10CE7:		//	𐳧	0xf0 0x90 0xb3 0xa7	OLD HUNGARIAN SMALL LETTER ENT
					*p = 0x10CA7;	//	𐲧	0xf0 0x90 0xb2 0xa7	OLD HUNGARIAN CAPITAL LETTER ENT
					break;
				case 0x10CE8:		//	𐳨	0xf0 0x90 0xb3 0xa8	OLD HUNGARIAN SMALL LETTER ETY
					*p = 0x10CA8;	//	𐲨	0xf0 0x90 0xb2 0xa8	OLD HUNGARIAN CAPITAL LETTER ETY
					break;
				case 0x10CE9:		//	𐳩	0xf0 0x90 0xb3 0xa9	OLD HUNGARIAN SMALL LETTER ECH
					*p = 0x10CA9;	//	𐲩	0xf0 0x90 0xb2 0xa9	OLD HUNGARIAN CAPITAL LETTER ECH
					break;
				case 0x10CEA:		//	𐳪	0xf0 0x90 0xb3 0xaa	OLD HUNGARIAN SMALL LETTER U
					*p = 0x10CAA;	//	𐲪	0xf0 0x90 0xb2 0xaa	OLD HUNGARIAN CAPITAL LETTER U
					break;
				case 0x10CEB:		//	𐳫	0xf0 0x90 0xb3 0xab	OLD HUNGARIAN SMALL LETTER UU
					*p = 0x10CAB;	//	𐲫	0xf0 0x90 0xb2 0xab	OLD HUNGARIAN CAPITAL LETTER UU
					break;
				case 0x10CEC:		//	𐳬	0xf0 0x90 0xb3 0xac	OLD HUNGARIAN SMALL LETTER NIKOLSBURG UE
					*p = 0x10CAC;	//	𐲬	0xf0 0x90 0xb2 0xac	OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG UE
					break;
				case 0x10CED:		//	𐳭	0xf0 0x90 0xb3 0xad	OLD HUNGARIAN SMALL LETTER RUDIMENTA UE
					*p = 0x10CAD;	//	𐲭	0xf0 0x90 0xb2 0xad	OLD HUNGARIAN CAPITAL LETTER RUDIMENTA UE
					break;
				case 0x10CEE:		//	𐳮	0xf0 0x90 0xb3 0xae	OLD HUNGARIAN SMALL LETTER EV
					*p = 0x10CAE;	//	𐲮	0xf0 0x90 0xb2 0xae	OLD HUNGARIAN CAPITAL LETTER EV
					break;
				case 0x10CEF:		//	𐳯	0xf0 0x90 0xb3 0xaf	OLD HUNGARIAN SMALL LETTER EZ
					*p = 0x10CAF;	//	𐲯	0xf0 0x90 0xb2 0xaf	OLD HUNGARIAN CAPITAL LETTER EZ
					break;
				case 0x10CF0:		//	𐳰	0xf0 0x90 0xb3 0xb0	OLD HUNGARIAN SMALL LETTER EZS
					*p = 0x10CB0;	//	𐲰	0xf0 0x90 0xb2 0xb0	OLD HUNGARIAN CAPITAL LETTER EZS
					break;
				case 0x10CF1:		//	𐳱	0xf0 0x90 0xb3 0xb1	OLD HUNGARIAN SMALL LETTER ENT-SHAPED SIGN
					*p = 0x10CB1;	//	𐲱	0xf0 0x90 0xb2 0xb1	OLD HUNGARIAN CAPITAL LETTER ENT-SHAPED SIGN
					break;
				case 0x10CF2:		//	𐳲	0xf0 0x90 0xb3 0xb2	OLD HUNGARIAN SMALL LETTER US
					*p = 0x10CB2;	//	𐲲	0xf0 0x90 0xb2 0xb2	OLD HUNGARIAN CAPITAL LETTER US
					break;
				case 0x118C0:		//	𑣀	0xf0 0x91 0xa3 0x80	WARANG CITI SMALL LETTER NGAA
					*p = 0x118A0;	//	𑢠	0xf0 0x91 0xa2 0xa0	WARANG CITI CAPITAL LETTER NGAA
					break;
				case 0x118C1:		//	𑣁	0xf0 0x91 0xa3 0x81	WARANG CITI SMALL LETTER A
					*p = 0x118A1;	//	𑢡	0xf0 0x91 0xa2 0xa1	WARANG CITI CAPITAL LETTER A
					break;
				case 0x118C2:		//	𑣂	0xf0 0x91 0xa3 0x82	WARANG CITI SMALL LETTER WI
					*p = 0x118A2;	//	𑢢	0xf0 0x91 0xa2 0xa2	WARANG CITI CAPITAL LETTER WI
					break;
				case 0x118C3:		//	𑣃	0xf0 0x91 0xa3 0x83	WARANG CITI SMALL LETTER YU
					*p = 0x118A3;	//	𑢣	0xf0 0x91 0xa2 0xa3	WARANG CITI CAPITAL LETTER YU
					break;
				case 0x118C4:		//	𑣄	0xf0 0x91 0xa3 0x84	WARANG CITI SMALL LETTER YA
					*p = 0x118A4;	//	𑢤	0xf0 0x91 0xa2 0xa4	WARANG CITI CAPITAL LETTER YA
					break;
				case 0x118C5:		//	𑣅	0xf0 0x91 0xa3 0x85	WARANG CITI SMALL LETTER YO
					*p = 0x118A5;	//	𑢥	0xf0 0x91 0xa2 0xa5	WARANG CITI CAPITAL LETTER YO
					break;
				case 0x118C6:		//	𑣆	0xf0 0x91 0xa3 0x86	WARANG CITI SMALL LETTER II
					*p = 0x118A6;	//	𑢦	0xf0 0x91 0xa2 0xa6	WARANG CITI CAPITAL LETTER II
					break;
				case 0x118C7:		//	𑣇	0xf0 0x91 0xa3 0x87	WARANG CITI SMALL LETTER UU
					*p = 0x118A7;	//	𑢧	0xf0 0x91 0xa2 0xa7	WARANG CITI CAPITAL LETTER UU
					break;
				case 0x118C8:		//	𑣈	0xf0 0x91 0xa3 0x88	WARANG CITI SMALL LETTER E
					*p = 0x118A8;	//	𑢨	0xf0 0x91 0xa2 0xa8	WARANG CITI CAPITAL LETTER E
					break;
				case 0x118C9:		//	𑣉	0xf0 0x91 0xa3 0x89	WARANG CITI SMALL LETTER O
					*p = 0x118A9;	//	𑢩	0xf0 0x91 0xa2 0xa9	WARANG CITI CAPITAL LETTER O
					break;
				case 0x118CA:		//	𑣊	0xf0 0x91 0xa3 0x8a	WARANG CITI SMALL LETTER ANG
					*p = 0x118AA;	//	𑢪	0xf0 0x91 0xa2 0xaa	WARANG CITI CAPITAL LETTER ANG
					break;
				case 0x118CB:		//	𑣋	0xf0 0x91 0xa3 0x8b	WARANG CITI SMALL LETTER GA
					*p = 0x118AB;	//	𑢫	0xf0 0x91 0xa2 0xab	WARANG CITI CAPITAL LETTER GA
					break;
				case 0x118CC:		//	𑣌	0xf0 0x91 0xa3 0x8c	WARANG CITI SMALL LETTER KO
					*p = 0x118AC;	//	𑢬	0xf0 0x91 0xa2 0xac	WARANG CITI CAPITAL LETTER KO
					break;
				case 0x118CD:		//	𑣍	0xf0 0x91 0xa3 0x8d	WARANG CITI SMALL LETTER ENY
					*p = 0x118AD;	//	𑢭	0xf0 0x91 0xa2 0xad	WARANG CITI CAPITAL LETTER ENY
					break;
				case 0x118CE:		//	𑣎	0xf0 0x91 0xa3 0x8e	WARANG CITI SMALL LETTER YUJ
					*p = 0x118AE;	//	𑢮	0xf0 0x91 0xa2 0xae	WARANG CITI CAPITAL LETTER YUJ
					break;
				case 0x118CF:		//	𑣏	0xf0 0x91 0xa3 0x8f	WARANG CITI SMALL LETTER UC
					*p = 0x118AF;	//	𑢯	0xf0 0x91 0xa2 0xaf	WARANG CITI CAPITAL LETTER UC
					break;
				case 0x118D0:		//	𑣐	0xf0 0x91 0xa3 0x90	WARANG CITI SMALL LETTER ENN
					*p = 0x118B0;	//	𑢰	0xf0 0x91 0xa2 0xb0	WARANG CITI CAPITAL LETTER ENN
					break;
				case 0x118D1:		//	𑣑	0xf0 0x91 0xa3 0x91	WARANG CITI SMALL LETTER ODD
					*p = 0x118B1;	//	𑢱	0xf0 0x91 0xa2 0xb1	WARANG CITI CAPITAL LETTER ODD
					break;
				case 0x118D2:		//	𑣒	0xf0 0x91 0xa3 0x92	WARANG CITI SMALL LETTER TTE
					*p = 0x118B2;	//	𑢲	0xf0 0x91 0xa2 0xb2	WARANG CITI CAPITAL LETTER TTE
					break;
				case 0x118D3:		//	𑣓	0xf0 0x91 0xa3 0x93	WARANG CITI SMALL LETTER NUNG
					*p = 0x118B3;	//	𑢳	0xf0 0x91 0xa2 0xb3	WARANG CITI CAPITAL LETTER NUNG
					break;
				case 0x118D4:		//	𑣔	0xf0 0x91 0xa3 0x94	WARANG CITI SMALL LETTER DA
					*p = 0x118B4;	//	𑢴	0xf0 0x91 0xa2 0xb4	WARANG CITI CAPITAL LETTER DA
					break;
				case 0x118D5:		//	𑣕	0xf0 0x91 0xa3 0x95	WARANG CITI SMALL LETTER AT
					*p = 0x118B5;	//	𑢵	0xf0 0x91 0xa2 0xb5	WARANG CITI CAPITAL LETTER AT
					break;
				case 0x118D6:		//	𑣖	0xf0 0x91 0xa3 0x96	WARANG CITI SMALL LETTER AM
					*p = 0x118B6;	//	𑢶	0xf0 0x91 0xa2 0xb6	WARANG CITI CAPITAL LETTER AM
					break;
				case 0x118D7:		//	𑣗	0xf0 0x91 0xa3 0x97	WARANG CITI SMALL LETTER BU
					*p = 0x118B7;	//	𑢷	0xf0 0x91 0xa2 0xb7	WARANG CITI CAPITAL LETTER BU
					break;
				case 0x118D8:		//	𑣘	0xf0 0x91 0xa3 0x98	WARANG CITI SMALL LETTER PU
					*p = 0x118B8;	//	𑢸	0xf0 0x91 0xa2 0xb8	WARANG CITI CAPITAL LETTER PU
					break;
				case 0x118D9:		//	𑣙	0xf0 0x91 0xa3 0x99	WARANG CITI SMALL LETTER HIYO
					*p = 0x118B9;	//	𑢹	0xf0 0x91 0xa2 0xb9	WARANG CITI CAPITAL LETTER HIYO
					break;
				case 0x118DA:		//	𑣚	0xf0 0x91 0xa3 0x9a	WARANG CITI SMALL LETTER HOLO
					*p = 0x118BA;	//	𑢺	0xf0 0x91 0xa2 0xba	WARANG CITI CAPITAL LETTER HOLO
					break;
				case 0x118DB:		//	𑣛	0xf0 0x91 0xa3 0x9b	WARANG CITI SMALL LETTER HORR
					*p = 0x118BB;	//	𑢻	0xf0 0x91 0xa2 0xbb	WARANG CITI CAPITAL LETTER HORR
					break;
				case 0x118DC:		//	𑣜	0xf0 0x91 0xa3 0x9c	WARANG CITI SMALL LETTER HAR
					*p = 0x118BC;	//	𑢼	0xf0 0x91 0xa2 0xbc	WARANG CITI CAPITAL LETTER HAR
					break;
				case 0x118DD:		//	𑣝	0xf0 0x91 0xa3 0x9d	WARANG CITI SMALL LETTER SSUU
					*p = 0x118BD;	//	𑢽	0xf0 0x91 0xa2 0xbd	WARANG CITI CAPITAL LETTER SSUU
					break;
				case 0x118DE:		//	𑣞	0xf0 0x91 0xa3 0x9e	WARANG CITI SMALL LETTER SII
					*p = 0x118BE;	//	𑢾	0xf0 0x91 0xa2 0xbe	WARANG CITI CAPITAL LETTER SII
					break;
				case 0x118DF:		//	𑣟	0xf0 0x91 0xa3 0x9f	WARANG CITI SMALL LETTER VIYO
					*p = 0x118BF;	//	𑢿	0xf0 0x91 0xa2 0xbf	WARANG CITI CAPITAL LETTER VIYO
					break;
				case 0x16E60:		//	𖹠	0xf0 0x96 0xb9 0xa0	MEDEFAIDRIN SMALL LETTER M
					*p = 0x16E40;	//	𖹀	0xf0 0x96 0xb9 0x80	MEDEFAIDRIN CAPITAL LETTER M
					break;
				case 0x16E61:		//	𖹡	0xf0 0x96 0xb9 0xa1	MEDEFAIDRIN SMALL LETTER S
					*p = 0x16E41;	//	𖹁	0xf0 0x96 0xb9 0x81	MEDEFAIDRIN CAPITAL LETTER S
					break;
				case 0x16E62:		//	𖹢	0xf0 0x96 0xb9 0xa2	MEDEFAIDRIN SMALL LETTER V
					*p = 0x16E42;	//	𖹂	0xf0 0x96 0xb9 0x82	MEDEFAIDRIN CAPITAL LETTER V
					break;
				case 0x16E63:		//	𖹣	0xf0 0x96 0xb9 0xa3	MEDEFAIDRIN SMALL LETTER W
					*p = 0x16E43;	//	𖹃	0xf0 0x96 0xb9 0x83	MEDEFAIDRIN CAPITAL LETTER W
					break;
				case 0x16E64:		//	𖹤	0xf0 0x96 0xb9 0xa4	MEDEFAIDRIN SMALL LETTER ATIU
					*p = 0x16E44;	//	𖹄	0xf0 0x96 0xb9 0x84	MEDEFAIDRIN CAPITAL LETTER ATIU
					break;
				case 0x16E65:		//	𖹥	0xf0 0x96 0xb9 0xa5	MEDEFAIDRIN SMALL LETTER Z
					*p = 0x16E45;	//	𖹅	0xf0 0x96 0xb9 0x85	MEDEFAIDRIN CAPITAL LETTER Z
					break;
				case 0x16E66:		//	𖹦	0xf0 0x96 0xb9 0xa6	MEDEFAIDRIN SMALL LETTER KP
					*p = 0x16E46;	//	𖹆	0xf0 0x96 0xb9 0x86	MEDEFAIDRIN CAPITAL LETTER KP
					break;
				case 0x16E67:		//	𖹧	0xf0 0x96 0xb9 0xa7	MEDEFAIDRIN SMALL LETTER P
					*p = 0x16E47;	//	𖹇	0xf0 0x96 0xb9 0x87	MEDEFAIDRIN CAPITAL LETTER P
					break;
				case 0x16E68:		//	𖹨	0xf0 0x96 0xb9 0xa8	MEDEFAIDRIN SMALL LETTER T
					*p = 0x16E48;	//	𖹈	0xf0 0x96 0xb9 0x88	MEDEFAIDRIN CAPITAL LETTER T
					break;
				case 0x16E69:		//	𖹩	0xf0 0x96 0xb9 0xa9	MEDEFAIDRIN SMALL LETTER G
					*p = 0x16E49;	//	𖹉	0xf0 0x96 0xb9 0x89	MEDEFAIDRIN CAPITAL LETTER G
					break;
				case 0x16E6A:		//	𖹪	0xf0 0x96 0xb9 0xaa	MEDEFAIDRIN SMALL LETTER F
					*p = 0x16E4A;	//	𖹊	0xf0 0x96 0xb9 0x8a	MEDEFAIDRIN CAPITAL LETTER F
					break;
				case 0x16E6B:		//	𖹫	0xf0 0x96 0xb9 0xab	MEDEFAIDRIN SMALL LETTER I
					*p = 0x16E4B;	//	𖹋	0xf0 0x96 0xb9 0x8b	MEDEFAIDRIN CAPITAL LETTER I
					break;
				case 0x16E6C:		//	𖹬	0xf0 0x96 0xb9 0xac	MEDEFAIDRIN SMALL LETTER K
					*p = 0x16E4C;	//	𖹌	0xf0 0x96 0xb9 0x8c	MEDEFAIDRIN CAPITAL LETTER K
					break;
				case 0x16E6D:		//	𖹭	0xf0 0x96 0xb9 0xad	MEDEFAIDRIN SMALL LETTER A
					*p = 0x16E4D;	//	𖹍	0xf0 0x96 0xb9 0x8d	MEDEFAIDRIN CAPITAL LETTER A
					break;
				case 0x16E6E:		//	𖹮	0xf0 0x96 0xb9 0xae	MEDEFAIDRIN SMALL LETTER J
					*p = 0x16E4E;	//	𖹎	0xf0 0x96 0xb9 0x8e	MEDEFAIDRIN CAPITAL LETTER J
					break;
				case 0x16E6F:		//	𖹯	0xf0 0x96 0xb9 0xaf	MEDEFAIDRIN SMALL LETTER E
					*p = 0x16E4F;	//	𖹏	0xf0 0x96 0xb9 0x8f	MEDEFAIDRIN CAPITAL LETTER E
					break;
				case 0x16E70:		//	𖹰	0xf0 0x96 0xb9 0xb0	MEDEFAIDRIN SMALL LETTER B
					*p = 0x16E50;	//	𖹐	0xf0 0x96 0xb9 0x90	MEDEFAIDRIN CAPITAL LETTER B
					break;
				case 0x16E71:		//	𖹱	0xf0 0x96 0xb9 0xb1	MEDEFAIDRIN SMALL LETTER C
					*p = 0x16E51;	//	𖹑	0xf0 0x96 0xb9 0x91	MEDEFAIDRIN CAPITAL LETTER C
					break;
				case 0x16E72:		//	𖹲	0xf0 0x96 0xb9 0xb2	MEDEFAIDRIN SMALL LETTER U
					*p = 0x16E52;	//	𖹒	0xf0 0x96 0xb9 0x92	MEDEFAIDRIN CAPITAL LETTER U
					break;
				case 0x16E73:		//	𖹳	0xf0 0x96 0xb9 0xb3	MEDEFAIDRIN SMALL LETTER YU
					*p = 0x16E53;	//	𖹓	0xf0 0x96 0xb9 0x93	MEDEFAIDRIN CAPITAL LETTER YU
					break;
				case 0x16E74:		//	𖹴	0xf0 0x96 0xb9 0xb4	MEDEFAIDRIN SMALL LETTER L
					*p = 0x16E54;	//	𖹔	0xf0 0x96 0xb9 0x94	MEDEFAIDRIN CAPITAL LETTER L
					break;
				case 0x16E75:		//	𖹵	0xf0 0x96 0xb9 0xb5	MEDEFAIDRIN SMALL LETTER Q
					*p = 0x16E55;	//	𖹕	0xf0 0x96 0xb9 0x95	MEDEFAIDRIN CAPITAL LETTER Q
					break;
				case 0x16E76:		//	𖹶	0xf0 0x96 0xb9 0xb6	MEDEFAIDRIN SMALL LETTER HP
					*p = 0x16E56;	//	𖹖	0xf0 0x96 0xb9 0x96	MEDEFAIDRIN CAPITAL LETTER HP
					break;
				case 0x16E77:		//	𖹷	0xf0 0x96 0xb9 0xb7	MEDEFAIDRIN SMALL LETTER NY
					*p = 0x16E57;	//	𖹗	0xf0 0x96 0xb9 0x97	MEDEFAIDRIN CAPITAL LETTER NY
					break;
				case 0x16E78:		//	𖹸	0xf0 0x96 0xb9 0xb8	MEDEFAIDRIN SMALL LETTER X
					*p = 0x16E58;	//	𖹘	0xf0 0x96 0xb9 0x98	MEDEFAIDRIN CAPITAL LETTER X
					break;
				case 0x16E79:		//	𖹹	0xf0 0x96 0xb9 0xb9	MEDEFAIDRIN SMALL LETTER D
					*p = 0x16E59;	//	𖹙	0xf0 0x96 0xb9 0x99	MEDEFAIDRIN CAPITAL LETTER D
					break;
				case 0x16E7A:		//	𖹺	0xf0 0x96 0xb9 0xba	MEDEFAIDRIN SMALL LETTER OE
					*p = 0x16E5A;	//	𖹚	0xf0 0x96 0xb9 0x9a	MEDEFAIDRIN CAPITAL LETTER OE
					break;
				case 0x16E7B:		//	𖹻	0xf0 0x96 0xb9 0xbb	MEDEFAIDRIN SMALL LETTER N
					*p = 0x16E5B;	//	𖹛	0xf0 0x96 0xb9 0x9b	MEDEFAIDRIN CAPITAL LETTER N
					break;
				case 0x16E7C:		//	𖹼	0xf0 0x96 0xb9 0xbc	MEDEFAIDRIN SMALL LETTER R
					*p = 0x16E5C;	//	𖹜	0xf0 0x96 0xb9 0x9c	MEDEFAIDRIN CAPITAL LETTER R
					break;
				case 0x16E7D:		//	𖹽	0xf0 0x96 0xb9 0xbd	MEDEFAIDRIN SMALL LETTER O
					*p = 0x16E5D;	//	𖹝	0xf0 0x96 0xb9 0x9d	MEDEFAIDRIN CAPITAL LETTER O
					break;
				case 0x16E7E:		//	𖹾	0xf0 0x96 0xb9 0xbe	MEDEFAIDRIN SMALL LETTER AI
					*p = 0x16E5E;	//	𖹞	0xf0 0x96 0xb9 0x9e	MEDEFAIDRIN CAPITAL LETTER AI
					break;
				case 0x16E7F:		//	𖹿	0xf0 0x96 0xb9 0xbf	MEDEFAIDRIN SMALL LETTER Y
					*p = 0x16E5F;	//	𖹟	0xf0 0x96 0xb9 0x9f	MEDEFAIDRIN CAPITAL LETTER Y
					break;
				case 0x1E922:		//	𞤢	0xf0 0x9e 0xa4 0xa2	ADLAM SMALL LETTER ALIF
					*p = 0x1E900;	//	𞤀	0xf0 0x9e 0xa4 0x80	ADLAM CAPITAL LETTER ALIF
					break;
				case 0x1E923:		//	𞤣	0xf0 0x9e 0xa4 0xa3	ADLAM SMALL LETTER DAALI
					*p = 0x1E901;	//	𞤁	0xf0 0x9e 0xa4 0x81	ADLAM CAPITAL LETTER DAALI
					break;
				case 0x1E924:		//	𞤤	0xf0 0x9e 0xa4 0xa4	ADLAM SMALL LETTER LAAM
					*p = 0x1E902;	//	𞤂	0xf0 0x9e 0xa4 0x82	ADLAM CAPITAL LETTER LAAM
					break;
				case 0x1E925:		//	𞤥	0xf0 0x9e 0xa4 0xa5	ADLAM SMALL LETTER MIIM
					*p = 0x1E903;	//	𞤃	0xf0 0x9e 0xa4 0x83	ADLAM CAPITAL LETTER MIIM
					break;
				case 0x1E926:		//	𞤦	0xf0 0x9e 0xa4 0xa6	ADLAM SMALL LETTER BA
					*p = 0x1E904;	//	𞤄	0xf0 0x9e 0xa4 0x84	ADLAM CAPITAL LETTER BA
					break;
				case 0x1E927:		//	𞤧	0xf0 0x9e 0xa4 0xa7	ADLAM SMALL LETTER SINNYIIYHE
					*p = 0x1E905;	//	𞤅	0xf0 0x9e 0xa4 0x85	ADLAM CAPITAL LETTER SINNYIIYHE
					break;
				case 0x1E928:		//	𞤨	0xf0 0x9e 0xa4 0xa8	ADLAM SMALL LETTER PE
					*p = 0x1E906;	//	𞤆	0xf0 0x9e 0xa4 0x86	ADLAM CAPITAL LETTER PE
					break;
				case 0x1E929:		//	𞤩	0xf0 0x9e 0xa4 0xa9	ADLAM SMALL LETTER BHE
					*p = 0x1E907;	//	𞤇	0xf0 0x9e 0xa4 0x87	ADLAM CAPITAL LETTER BHE
					break;
				case 0x1E92A:		//	𞤪	0xf0 0x9e 0xa4 0xaa	ADLAM SMALL LETTER RA
					*p = 0x1E908;	//	𞤈	0xf0 0x9e 0xa4 0x88	ADLAM CAPITAL LETTER RA
					break;
				case 0x1E92B:		//	𞤫	0xf0 0x9e 0xa4 0xab	ADLAM SMALL LETTER E
					*p = 0x1E909;	//	𞤉	0xf0 0x9e 0xa4 0x89	ADLAM CAPITAL LETTER E
					break;
				case 0x1E92C:		//	𞤬	0xf0 0x9e 0xa4 0xac	ADLAM SMALL LETTER FA
					*p = 0x1E90A;	//	𞤊	0xf0 0x9e 0xa4 0x8a	ADLAM CAPITAL LETTER FA
					break;
				case 0x1E92D:		//	𞤭	0xf0 0x9e 0xa4 0xad	ADLAM SMALL LETTER I
					*p = 0x1E90B;	//	𞤋	0xf0 0x9e 0xa4 0x8b	ADLAM CAPITAL LETTER I
					break;
				case 0x1E92E:		//	𞤮	0xf0 0x9e 0xa4 0xae	ADLAM SMALL LETTER O
					*p = 0x1E90C;	//	𞤌	0xf0 0x9e 0xa4 0x8c	ADLAM CAPITAL LETTER O
					break;
				case 0x1E92F:		//	𞤯	0xf0 0x9e 0xa4 0xaf	ADLAM SMALL LETTER DHA
					*p = 0x1E90D;	//	𞤍	0xf0 0x9e 0xa4 0x8d	ADLAM CAPITAL LETTER DHA
					break;
				case 0x1E930:		//	𞤰	0xf0 0x9e 0xa4 0xb0	ADLAM SMALL LETTER YHE
					*p = 0x1E90E;	//	𞤎	0xf0 0x9e 0xa4 0x8e	ADLAM CAPITAL LETTER YHE
					break;
				case 0x1E931:		//	𞤱	0xf0 0x9e 0xa4 0xb1	ADLAM SMALL LETTER WAW
					*p = 0x1E90F;	//	𞤏	0xf0 0x9e 0xa4 0x8f	ADLAM CAPITAL LETTER WAW
					break;
				case 0x1E932:		//	𞤲	0xf0 0x9e 0xa4 0xb2	ADLAM SMALL LETTER NUN
					*p = 0x1E910;	//	𞤐	0xf0 0x9e 0xa4 0x90	ADLAM CAPITAL LETTER NUN
					break;
				case 0x1E933:		//	𞤳	0xf0 0x9e 0xa4 0xb3	ADLAM SMALL LETTER KAF
					*p = 0x1E911;	//	𞤑	0xf0 0x9e 0xa4 0x91	ADLAM CAPITAL LETTER KAF
					break;
				case 0x1E934:		//	𞤴	0xf0 0x9e 0xa4 0xb4	ADLAM SMALL LETTER YA
					*p = 0x1E912;	//	𞤒	0xf0 0x9e 0xa4 0x92	ADLAM CAPITAL LETTER YA
					break;
				case 0x1E935:		//	𞤵	0xf0 0x9e 0xa4 0xb5	ADLAM SMALL LETTER U
					*p = 0x1E913;	//	𞤓	0xf0 0x9e 0xa4 0x93	ADLAM CAPITAL LETTER U
					break;
				case 0x1E936:		//	𞤶	0xf0 0x9e 0xa4 0xb6	ADLAM SMALL LETTER JIIM
					*p = 0x1E914;	//	𞤔	0xf0 0x9e 0xa4 0x94	ADLAM CAPITAL LETTER JIIM
					break;
				case 0x1E937:		//	𞤷	0xf0 0x9e 0xa4 0xb7	ADLAM SMALL LETTER CHI
					*p = 0x1E915;	//	𞤕	0xf0 0x9e 0xa4 0x95	ADLAM CAPITAL LETTER CHI
					break;
				case 0x1E938:		//	𞤸	0xf0 0x9e 0xa4 0xb8	ADLAM SMALL LETTER HA
					*p = 0x1E916;	//	𞤖	0xf0 0x9e 0xa4 0x96	ADLAM CAPITAL LETTER HA
					break;
				case 0x1E939:		//	𞤹	0xf0 0x9e 0xa4 0xb9	ADLAM SMALL LETTER QAAF
					*p = 0x1E917;	//	𞤗	0xf0 0x9e 0xa4 0x97	ADLAM CAPITAL LETTER QAAF
					break;
				case 0x1E93A:		//	𞤺	0xf0 0x9e 0xa4 0xba	ADLAM SMALL LETTER GA
					*p = 0x1E918;	//	𞤘	0xf0 0x9e 0xa4 0x98	ADLAM CAPITAL LETTER GA
					break;
				case 0x1E93B:		//	𞤻	0xf0 0x9e 0xa4 0xbb	ADLAM SMALL LETTER NYA
					*p = 0x1E919;	//	𞤙	0xf0 0x9e 0xa4 0x99	ADLAM CAPITAL LETTER NYA
					break;
				case 0x1E93C:		//	𞤼	0xf0 0x9e 0xa4 0xbc	ADLAM SMALL LETTER TU
					*p = 0x1E91A;	//	𞤚	0xf0 0x9e 0xa4 0x9a	ADLAM CAPITAL LETTER TU
					break;
				case 0x1E93D:		//	𞤽	0xf0 0x9e 0xa4 0xbd	ADLAM SMALL LETTER NHA
					*p = 0x1E91B;	//	𞤛	0xf0 0x9e 0xa4 0x9b	ADLAM CAPITAL LETTER NHA
					break;
				case 0x1E93E:		//	𞤾	0xf0 0x9e 0xa4 0xbe	ADLAM SMALL LETTER VA
					*p = 0x1E91C;	//	𞤜	0xf0 0x9e 0xa4 0x9c	ADLAM CAPITAL LETTER VA
					break;
				case 0x1E93F:		//	𞤿	0xf0 0x9e 0xa4 0xbf	ADLAM SMALL LETTER KHA
					*p = 0x1E91D;	//	𞤝	0xf0 0x9e 0xa4 0x9d	ADLAM CAPITAL LETTER KHA
					break;
				case 0x1E940:		//	𞥀	0xf0 0x9e 0xa5 0x80	ADLAM SMALL LETTER GBE
					*p = 0x1E91E;	//	𞤞	0xf0 0x9e 0xa4 0x9e	ADLAM CAPITAL LETTER GBE
					break;
				case 0x1E941:		//	𞥁	0xf0 0x9e 0xa5 0x81	ADLAM SMALL LETTER ZAL
					*p = 0x1E91F;	//	𞤟	0xf0 0x9e 0xa4 0x9f	ADLAM CAPITAL LETTER ZAL
					break;
				case 0x1E942:		//	𞥂	0xf0 0x9e 0xa5 0x82	ADLAM SMALL LETTER KPO
					*p = 0x1E920;	//	𞤠	0xf0 0x9e 0xa4 0xa0	ADLAM CAPITAL LETTER KPO
					break;
				case 0x1E943:		//	𞥃	0xf0 0x9e 0xa5 0x83	ADLAM SMALL LETTER SHA
					*p = 0x1E921;	//	𞤡	0xf0 0x9e 0xa4 0xa1	ADLAM CAPITAL LETTER SHA
					break;
				}
			}
		p++;
		}
	}
	return pUtf32;
}
Utf32Char* StrToLwrUtf32(Utf32Char* pUtf32)
{
	Utf32Char* p = pUtf32;
	if (pUtf32 && *pUtf32) {
		while(*p) {
			if ((*p >= 0x41) && (*p <= 0x5a)) /* US ASCII */
				(*p) += 0x20;
			else {
				switch(*p) {
				case 0x00C0:		//	À	0xc3 0x80	LATIN CAPITAL LETTER A WITH GRAVE
					*p = 0x00E0;	//	à	0xc3 0xa0	LATIN SMALL LETTER A WITH GRAVE
					break;
				case 0x00C1:		//	Á	0xc3 0x81	LATIN CAPITAL LETTER A WITH ACUTE
					*p = 0x00E1;	//	á	0xc3 0xa1	LATIN SMALL LETTER A WITH ACUTE
					break;
				case 0x00C2:		//	Â	0xc3 0x82	LATIN CAPITAL LETTER A WITH CIRCUMFLEX
					*p = 0x00E2;	//	â	0xc3 0xa2	LATIN SMALL LETTER A WITH CIRCUMFLEX
					break;
				case 0x00C3:		//	Ã	0xc3 0x83	LATIN CAPITAL LETTER A WITH TILDE
					*p = 0x00E3;	//	ã	0xc3 0xa3	LATIN SMALL LETTER A WITH TILDE
					break;
				case 0x00C4:		//	Ä	0xc3 0x84	LATIN CAPITAL LETTER A WITH DIAERESIS
					*p = 0x00E4;	//	ä	0xc3 0xa4	LATIN SMALL LETTER A WITH DIAERESIS
					break;
				case 0x00C5:		//	Å	0xc3 0x85	LATIN CAPITAL LETTER A WITH RING ABOVE
					*p = 0x00E5;	//	å	0xc3 0xa5	LATIN SMALL LETTER A WITH RING ABOVE
					break;
				case 0x00C6:		//	Æ	0xc3 0x86	LATIN CAPITAL LETTER AE
					*p = 0x00E6;	//	æ	0xc3 0xa6	LATIN SMALL LETTER AE
					break;
				case 0x00C7:		//	Ç	0xc3 0x87	LATIN CAPITAL LETTER C WITH CEDILLA
					*p = 0x00E7;	//	ç	0xc3 0xa7	LATIN SMALL LETTER C WITH CEDILLA
					break;
				case 0x00C8:		//	È	0xc3 0x88	LATIN CAPITAL LETTER E WITH GRAVE
					*p = 0x00E8;	//	è	0xc3 0xa8	LATIN SMALL LETTER E WITH GRAVE
					break;
				case 0x00C9:		//	É	0xc3 0x89	LATIN CAPITAL LETTER E WITH ACUTE
					*p = 0x00E9;	//	é	0xc3 0xa9	LATIN SMALL LETTER E WITH ACUTE
					break;
				case 0x00CA:		//	Ê	0xc3 0x8a	LATIN CAPITAL LETTER E WITH CIRCUMFLEX
					*p = 0x00EA;	//	ê	0xc3 0xaa	LATIN SMALL LETTER E WITH CIRCUMFLEX
					break;
				case 0x00CB:		//	Ë	0xc3 0x8b	LATIN CAPITAL LETTER E WITH DIAERESIS
					*p = 0x00EB;	//	ë	0xc3 0xab	LATIN SMALL LETTER E WITH DIAERESIS
					break;
				case 0x00CC:		//	Ì	0xc3 0x8c	LATIN CAPITAL LETTER I WITH GRAVE
					*p = 0x00EC;	//	ì	0xc3 0xac	LATIN SMALL LETTER I WITH GRAVE
					break;
				case 0x00CD:		//	Í	0xc3 0x8d	LATIN CAPITAL LETTER I WITH ACUTE
					*p = 0x00ED;	//	í	0xc3 0xad	LATIN SMALL LETTER I WITH ACUTE
					break;
				case 0x00CE:		//	Î	0xc3 0x8e	LATIN CAPITAL LETTER I WITH CIRCUMFLEX
					*p = 0x00EE;	//	î	0xc3 0xae	LATIN SMALL LETTER I WITH CIRCUMFLEX
					break;
				case 0x00CF:		//	Ï	0xc3 0x8f	LATIN CAPITAL LETTER I WITH DIAERESIS
					*p = 0x00EF;	//	ï	0xc3 0xaf	LATIN SMALL LETTER I WITH DIAERESIS
					break;
				case 0x00D0:		//	Ð	0xc3 0x90	LATIN CAPITAL LETTER ETH
					*p = 0x00F0;	//	ð	0xc3 0xb0	LATIN SMALL LETTER ETH
					break;
				case 0x00D1:		//	Ñ	0xc3 0x91	LATIN CAPITAL LETTER N WITH TILDE
					*p = 0x00F1;	//	ñ	0xc3 0xb1	LATIN SMALL LETTER N WITH TILDE
					break;
				case 0x00D2:		//	Ò	0xc3 0x92	LATIN CAPITAL LETTER O WITH GRAVE
					*p = 0x00F2;	//	ò	0xc3 0xb2	LATIN SMALL LETTER O WITH GRAVE
					break;
				case 0x00D3:		//	Ó	0xc3 0x93	LATIN CAPITAL LETTER O WITH ACUTE
					*p = 0x00F3;	//	ó	0xc3 0xb3	LATIN SMALL LETTER O WITH ACUTE
					break;
				case 0x00D4:		//	Ô	0xc3 0x94	LATIN CAPITAL LETTER O WITH CIRCUMFLEX
					*p = 0x00F4;	//	ô	0xc3 0xb4	LATIN SMALL LETTER O WITH CIRCUMFLEX
					break;
				case 0x00D5:		//	Õ	0xc3 0x95	LATIN CAPITAL LETTER O WITH TILDE
					*p = 0x00F5;	//	õ	0xc3 0xb5	LATIN SMALL LETTER O WITH TILDE
					break;
				case 0x00D6:		//	Ö	0xc3 0x96	LATIN CAPITAL LETTER O WITH DIAERESIS
					*p = 0x00F6;	//	ö	0xc3 0xb6	LATIN SMALL LETTER O WITH DIAERESIS
					break;
				case 0x00D8:		//	Ø	0xc3 0x98	LATIN CAPITAL LETTER O WITH STROKE
					*p = 0x00F8;	//	ø	0xc3 0xb8	LATIN SMALL LETTER O WITH STROKE
					break;
				case 0x00D9:		//	Ù	0xc3 0x99	LATIN CAPITAL LETTER U WITH GRAVE
					*p = 0x00F9;	//	ù	0xc3 0xb9	LATIN SMALL LETTER U WITH GRAVE
					break;
				case 0x00DA:		//	Ú	0xc3 0x9a	LATIN CAPITAL LETTER U WITH ACUTE
					*p = 0x00FA;	//	ú	0xc3 0xba	LATIN SMALL LETTER U WITH ACUTE
					break;
				case 0x00DB:		//	Û	0xc3 0x9b	LATIN CAPITAL LETTER U WITH CIRCUMFLEX
					*p = 0x00FB;	//	û	0xc3 0xbb	LATIN SMALL LETTER U WITH CIRCUMFLEX
					break;
				case 0x00DC:		//	Ü	0xc3 0x9c	LATIN CAPITAL LETTER U WITH DIAERESIS
					*p = 0x00FC;	//	ü	0xc3 0xbc	LATIN SMALL LETTER U WITH DIAERESIS
					break;
				case 0x00DD:		//	Ý	0xc3 0x9d	LATIN CAPITAL LETTER Y WITH ACUTE
					*p = 0x00FD;	//	ý	0xc3 0xbd	LATIN SMALL LETTER Y WITH ACUTE
					break;
				case 0x00DE:		//	Þ	0xc3 0x9e	LATIN CAPITAL LETTER THORN
					*p = 0x00FE;	//	þ	0xc3 0xbe	LATIN SMALL LETTER THORN
					break;
				case 0x0100:		//	Ā	0xc4 0x80	LATIN CAPITAL LETTER A WITH MACRON
					*p = 0x0101;	//	ā	0xc4 0x81	LATIN SMALL LETTER A WITH MACRON
					break;
				case 0x0102:		//	Ă	0xc4 0x82	LATIN CAPITAL LETTER A WITH BREVE
					*p = 0x0103;	//	ă	0xc4 0x83	LATIN SMALL LETTER A WITH BREVE
					break;
				case 0x0104:		//	Ą	0xc4 0x84	LATIN CAPITAL LETTER A WITH OGONEK
					*p = 0x0105;	//	ą	0xc4 0x85	LATIN SMALL LETTER A WITH OGONEK
					break;
				case 0x0106:		//	Ć	0xc4 0x86	LATIN CAPITAL LETTER C WITH ACUTE
					*p = 0x0107;	//	ć	0xc4 0x87	LATIN SMALL LETTER C WITH ACUTE
					break;
				case 0x0108:		//	Ĉ	0xc4 0x88	LATIN CAPITAL LETTER C WITH CIRCUMFLEX
					*p = 0x0109;	//	ĉ	0xc4 0x89	LATIN SMALL LETTER C WITH CIRCUMFLEX
					break;
				case 0x010A:		//	Ċ	0xc4 0x8a	LATIN CAPITAL LETTER C WITH DOT ABOVE
					*p = 0x010B;	//	ċ	0xc4 0x8b	LATIN SMALL LETTER C WITH DOT ABOVE
					break;
				case 0x010C:		//	Č	0xc4 0x8c	LATIN CAPITAL LETTER C WITH CARON
					*p = 0x010D;	//	č	0xc4 0x8d	LATIN SMALL LETTER C WITH CARON
					break;
				case 0x010E:		//	Ď	0xc4 0x8e	LATIN CAPITAL LETTER D WITH CARON
					*p = 0x010F;	//	ď	0xc4 0x8f	LATIN SMALL LETTER D WITH CARON
					break;
				case 0x0110:		//	Đ	0xc4 0x90	LATIN CAPITAL LETTER D WITH STROKE
					*p = 0x0111;	//	đ	0xc4 0x91	LATIN SMALL LETTER D WITH STROKE
					break;
				case 0x0112:		//	Ē	0xc4 0x92	LATIN CAPITAL LETTER E WITH MACRON
					*p = 0x0113;	//	ē	0xc4 0x93	LATIN SMALL LETTER E WITH MACRON
					break;
				case 0x0114:		//	Ĕ	0xc4 0x94	LATIN CAPITAL LETTER E WITH BREVE
					*p = 0x0115;	//	ĕ	0xc4 0x95	LATIN SMALL LETTER E WITH BREVE
					break;
				case 0x0116:		//	Ė	0xc4 0x96	LATIN CAPITAL LETTER E WITH DOT ABOVE
					*p = 0x0117;	//	ė	0xc4 0x97	LATIN SMALL LETTER E WITH DOT ABOVE
					break;
				case 0x0118:		//	Ę	0xc4 0x98	LATIN CAPITAL LETTER E WITH OGONEK
					*p = 0x0119;	//	ę	0xc4 0x99	LATIN SMALL LETTER E WITH OGONEK
					break;
				case 0x011A:		//	Ě	0xc4 0x9a	LATIN CAPITAL LETTER E WITH CARON
					*p = 0x011B;	//	ě	0xc4 0x9b	LATIN SMALL LETTER E WITH CARON
					break;
				case 0x011C:		//	Ĝ	0xc4 0x9c	LATIN CAPITAL LETTER G WITH CIRCUMFLEX
					*p = 0x011D;	//	ĝ	0xc4 0x9d	LATIN SMALL LETTER G WITH CIRCUMFLEX
					break;
				case 0x011E:		//	Ğ	0xc4 0x9e	LATIN CAPITAL LETTER G WITH BREVE
					*p = 0x011F;	//	ğ	0xc4 0x9f	LATIN SMALL LETTER G WITH BREVE
					break;
				case 0x0120:		//	Ġ	0xc4 0xa0	LATIN CAPITAL LETTER G WITH DOT ABOVE
					*p = 0x0121;	//	ġ	0xc4 0xa1	LATIN SMALL LETTER G WITH DOT ABOVE
					break;
				case 0x0122:		//	Ģ	0xc4 0xa2	LATIN CAPITAL LETTER G WITH CEDILLA
					*p = 0x0123;	//	ģ	0xc4 0xa3	LATIN SMALL LETTER G WITH CEDILLA
					break;
				case 0x0124:		//	Ĥ	0xc4 0xa4	LATIN CAPITAL LETTER H WITH CIRCUMFLEX
					*p = 0x0125;	//	ĥ	0xc4 0xa5	LATIN SMALL LETTER H WITH CIRCUMFLEX
					break;
				case 0x0126:		//	Ħ	0xc4 0xa6	LATIN CAPITAL LETTER H WITH STROKE
					*p = 0x0127;	//	ħ	0xc4 0xa7	LATIN SMALL LETTER H WITH STROKE
					break;
				case 0x0128:		//	Ĩ	0xc4 0xa8	LATIN CAPITAL LETTER I WITH TILDE
					*p = 0x0129;	//	ĩ	0xc4 0xa9	LATIN SMALL LETTER I WITH TILDE
					break;
				case 0x012A:		//	Ī	0xc4 0xaa	LATIN CAPITAL LETTER I WITH MACRON
					*p = 0x012B;	//	ī	0xc4 0xab	LATIN SMALL LETTER I WITH MACRON
					break;
				case 0x012C:		//	Ĭ	0xc4 0xac	LATIN CAPITAL LETTER I WITH BREVE
					*p = 0x012D;	//	ĭ	0xc4 0xad	LATIN SMALL LETTER I WITH BREVE
					break;
				case 0x012E:		//	Į	0xc4 0xae	LATIN CAPITAL LETTER I WITH OGONEK
					*p = 0x012F;	//	į	0xc4 0xaf	LATIN SMALL LETTER I WITH OGONEK
					break;
				case 0x0130:		//	İ	0xc4 0xb0	LATIN CAPITAL LETTER I WITH DOT ABOVE
					*p = 0x0069;	//	i	0x69	LATIN SMALL LETTER I
					break;
				case 0x0132:		//	Ĳ	0xc4 0xb2	LATIN CAPITAL LETTER LIGATURE IJ
					*p = 0x0133;	//	ĳ	0xc4 0xb3	LATIN SMALL LETTER LIGATURE IJ
					break;
				case 0x0134:		//	Ĵ	0xc4 0xb4	LATIN CAPITAL LETTER J WITH CIRCUMFLEX
					*p = 0x0135;	//	ĵ	0xc4 0xb5	LATIN SMALL LETTER J WITH CIRCUMFLEX
					break;
				case 0x0136:		//	Ķ	0xc4 0xb6	LATIN CAPITAL LETTER K WITH CEDILLA
					*p = 0x0137;	//	ķ	0xc4 0xb7	LATIN SMALL LETTER K WITH CEDILLA
					break;
				case 0x0139:		//	Ĺ	0xc4 0xb9	LATIN CAPITAL LETTER L WITH ACUTE
					*p = 0x013A;	//	ĺ	0xc4 0xba	LATIN SMALL LETTER L WITH ACUTE
					break;
				case 0x013B:		//	Ļ	0xc4 0xbb	LATIN CAPITAL LETTER L WITH CEDILLA
					*p = 0x013C;	//	ļ	0xc4 0xbc	LATIN SMALL LETTER L WITH CEDILLA
					break;
				case 0x013D:		//	Ľ	0xc4 0xbd	LATIN CAPITAL LETTER L WITH CARON
					*p = 0x013E;	//	ľ	0xc4 0xbe	LATIN SMALL LETTER L WITH CARON
					break;
				case 0x013F:		//	Ŀ	0xc4 0xbf	LATIN CAPITAL LETTER L WITH MIDDLE DOT
					*p = 0x0140;	//	ŀ	0xc5 0x80	LATIN SMALL LETTER L WITH MIDDLE DOT
					break;
				case 0x0141:		//	Ł	0xc5 0x81	LATIN CAPITAL LETTER L WITH STROKE
					*p = 0x0142;	//	ł	0xc5 0x82	LATIN SMALL LETTER L WITH STROKE
					break;
				case 0x0143:		//	Ń	0xc5 0x83	LATIN CAPITAL LETTER N WITH ACUTE
					*p = 0x0144;	//	ń	0xc5 0x84	LATIN SMALL LETTER N WITH ACUTE
					break;
				case 0x0145:		//	Ņ	0xc5 0x85	LATIN CAPITAL LETTER N WITH CEDILLA
					*p = 0x0146;	//	ņ	0xc5 0x86	LATIN SMALL LETTER N WITH CEDILLA
					break;
				case 0x0147:		//	Ň	0xc5 0x87	LATIN CAPITAL LETTER N WITH CARON
					*p = 0x0148;	//	ň	0xc5 0x88	LATIN SMALL LETTER N WITH CARON
					break;
				case 0x014A:		//	Ŋ	0xc5 0x8a	LATIN CAPITAL LETTER ENG
					*p = 0x014B;	//	ŋ	0xc5 0x8b	LATIN SMALL LETTER ENG
					break;
				case 0x014C:		//	Ō	0xc5 0x8c	LATIN CAPITAL LETTER O WITH MACRON
					*p = 0x014D;	//	ō	0xc5 0x8d	LATIN SMALL LETTER O WITH MACRON
					break;
				case 0x014E:		//	Ŏ	0xc5 0x8e	LATIN CAPITAL LETTER O WITH BREVE
					*p = 0x014F;	//	ŏ	0xc5 0x8f	LATIN SMALL LETTER O WITH BREVE
					break;
				case 0x0150:		//	Ő	0xc5 0x90	LATIN CAPITAL LETTER O WITH DOUBLE ACUTE
					*p = 0x0151;	//	ő	0xc5 0x91	LATIN SMALL LETTER O WITH DOUBLE ACUTE
					break;
				case 0x0152:		//	Œ	0xc5 0x92	LATIN CAPITAL LETTER LIGATURE OE
					*p = 0x0153;	//	œ	0xc5 0x93	LATIN SMALL LETTER LIGATURE OE
					break;
				case 0x0154:		//	Ŕ	0xc5 0x94	LATIN CAPITAL LETTER R WITH ACUTE
					*p = 0x0155;	//	ŕ	0xc5 0x95	LATIN SMALL LETTER R WITH ACUTE
					break;
				case 0x0156:		//	Ŗ	0xc5 0x96	LATIN CAPITAL LETTER R WITH CEDILLA
					*p = 0x0157;	//	ŗ	0xc5 0x97	LATIN SMALL LETTER R WITH CEDILLA
					break;
				case 0x0158:		//	Ř	0xc5 0x98	LATIN CAPITAL LETTER R WITH CARON
					*p = 0x0159;	//	ř	0xc5 0x99	LATIN SMALL LETTER R WITH CARON
					break;
				case 0x015A:		//	Ś	0xc5 0x9a	LATIN CAPITAL LETTER S WITH ACUTE
					*p = 0x015B;	//	ś	0xc5 0x9b	LATIN SMALL LETTER S WITH ACUTE
					break;
				case 0x015C:		//	Ŝ	0xc5 0x9c	LATIN CAPITAL LETTER S WITH CIRCUMFLEX
					*p = 0x015D;	//	ŝ	0xc5 0x9d	LATIN SMALL LETTER S WITH CIRCUMFLEX
					break;
				case 0x015E:		//	Ş	0xc5 0x9e	LATIN CAPITAL LETTER S WITH CEDILLA
					*p = 0x015F;	//	ş	0xc5 0x9f	LATIN SMALL LETTER S WITH CEDILLA
					break;
				case 0x0160:		//	Š	0xc5 0xa0	LATIN CAPITAL LETTER S WITH CARON
					*p = 0x0161;	//	š	0xc5 0xa1	LATIN SMALL LETTER S WITH CARON
					break;
				case 0x0162:		//	Ţ	0xc5 0xa2	LATIN CAPITAL LETTER T WITH CEDILLA
					*p = 0x0163;	//	ţ	0xc5 0xa3	LATIN SMALL LETTER T WITH CEDILLA
					break;
				case 0x0164:		//	Ť	0xc5 0xa4	LATIN CAPITAL LETTER T WITH CARON
					*p = 0x0165;	//	ť	0xc5 0xa5	LATIN SMALL LETTER T WITH CARON
					break;
				case 0x0166:		//	Ŧ	0xc5 0xa6	LATIN CAPITAL LETTER T WITH STROKE
					*p = 0x0167;	//	ŧ	0xc5 0xa7	LATIN SMALL LETTER T WITH STROKE
					break;
				case 0x0168:		//	Ũ	0xc5 0xa8	LATIN CAPITAL LETTER U WITH TILDE
					*p = 0x0169;	//	ũ	0xc5 0xa9	LATIN SMALL LETTER U WITH TILDE
					break;
				case 0x016A:		//	Ū	0xc5 0xaa	LATIN CAPITAL LETTER U WITH MACRON
					*p = 0x016B;	//	ū	0xc5 0xab	LATIN SMALL LETTER U WITH MACRON
					break;
				case 0x016C:		//	Ŭ	0xc5 0xac	LATIN CAPITAL LETTER U WITH BREVE
					*p = 0x016D;	//	ŭ	0xc5 0xad	LATIN SMALL LETTER U WITH BREVE
					break;
				case 0x016E:		//	Ů	0xc5 0xae	LATIN CAPITAL LETTER U WITH RING ABOVE
					*p = 0x016F;	//	ů	0xc5 0xaf	LATIN SMALL LETTER U WITH RING ABOVE
					break;
				case 0x0170:		//	Ű	0xc5 0xb0	LATIN CAPITAL LETTER U WITH DOUBLE ACUTE
					*p = 0x0171;	//	ű	0xc5 0xb1	LATIN SMALL LETTER U WITH DOUBLE ACUTE
					break;
				case 0x0172:		//	Ų	0xc5 0xb2	LATIN CAPITAL LETTER U WITH OGONEK
					*p = 0x0173;	//	ų	0xc5 0xb3	LATIN SMALL LETTER U WITH OGONEK
					break;
				case 0x0174:		//	Ŵ	0xc5 0xb4	LATIN CAPITAL LETTER W WITH CIRCUMFLEX
					*p = 0x0175;	//	ŵ	0xc5 0xb5	LATIN SMALL LETTER W WITH CIRCUMFLEX
					break;
				case 0x0176:		//	Ŷ	0xc5 0xb6	LATIN CAPITAL LETTER Y WITH CIRCUMFLEX
					*p = 0x0177;	//	ŷ	0xc5 0xb7	LATIN SMALL LETTER Y WITH CIRCUMFLEX
					break;
				case 0x0178:		//	Ÿ	0xc5 0xb8	LATIN CAPITAL LETTER Y WITH DIAERESIS
					*p = 0x00FF;	//	ÿ	0xc3 0xbf	LATIN SMALL LETTER Y WITH DIAERESIS
					break;
				case 0x0179:		//	Ź	0xc5 0xb9	LATIN CAPITAL LETTER Z WITH ACUTE
					*p = 0x017A;	//	ź	0xc5 0xba	LATIN SMALL LETTER Z WITH ACUTE
					break;
				case 0x017B:		//	Ż	0xc5 0xbb	LATIN CAPITAL LETTER Z WITH DOT ABOVE
					*p = 0x017C;	//	ż	0xc5 0xbc	LATIN SMALL LETTER Z WITH DOT ABOVE
					break;
				case 0x017D:		//	Ž	0xc5 0xbd	LATIN CAPITAL LETTER Z WITH CARON
					*p = 0x017E;	//	ž	0xc5 0xbe	LATIN SMALL LETTER Z WITH CARON
					break;
				case 0x0181:		//	Ɓ	0xc6 0x81	LATIN CAPITAL LETTER B WITH HOOK
					*p = 0x0253;	//	ɓ	0xc9 0x93	LATIN SMALL LETTER B WITH HOOK
					break;
				case 0x0182:		//	Ƃ	0xc6 0x82	LATIN CAPITAL LETTER B WITH TOPBAR
					*p = 0x0183;	//	ƃ	0xc6 0x83	LATIN SMALL LETTER B WITH TOPBAR
					break;
				case 0x0184:		//	Ƅ	0xc6 0x84	LATIN CAPITAL LETTER TONE SIX
					*p = 0x0185;	//	ƅ	0xc6 0x85	LATIN SMALL LETTER TONE SIX
					break;
				case 0x0186:		//	Ɔ	0xc6 0x86	LATIN CAPITAL LETTER OPEN O
					*p = 0x0254;	//	ɔ	0xc9 0x94	LATIN SMALL LETTER OPEN O
					break;
				case 0x0187:		//	Ƈ	0xc6 0x87	LATIN CAPITAL LETTER C WITH HOOK
					*p = 0x0188;	//	ƈ	0xc6 0x88	LATIN SMALL LETTER C WITH HOOK
					break;
				case 0x0189:		//	Ɖ	0xc6 0x89	LATIN CAPITAL LETTER AFRICAN D
					*p = 0x0256;	//	ɖ	0xc9 0x96	LATIN SMALL LETTER AFRICAN D
					break;
				case 0x018A:		//	Ɗ	0xc6 0x8a	LATIN CAPITAL LETTER D WITH HOOK
					*p = 0x0257;	//	ɗ	0xc9 0x97	LATIN SMALL LETTER D WITH HOOK
					break;
				case 0x018B:		//	Ƌ	0xc6 0x8b	LATIN CAPITAL LETTER D WITH TOPBAR
					*p = 0x018C;	//	ƌ	0xc6 0x8c	LATIN SMALL LETTER D WITH TOPBAR
					break;
				case 0x018E:		//	Ǝ	0xc6 0x8e	LATIN CAPITAL LETTER REVERSED E
					*p = 0x01DD;	//	ǝ	0xC7 0x9D	LATIN SMALL LETTER TURNED E (look mid page 297 https://www.unicode.org/versions/Unicode14.0.0/ch07.pdf)
					break;
				case 0x018F:		//	Ə	0xc6 0x8f	LATIN CAPITAL LETTER SCHWA
					*p = 0x0259;	//	ə	0xc9 0x99	LATIN SMALL LETTER SCHWA
					break;
				case 0x0190:		//	Ɛ	0xc6 0x90	LATIN CAPITAL LETTER OPEN E
					*p = 0x025B;	//	ɛ	0xc9 0x9b	LATIN SMALL LETTER OPEN E
					break;
				case 0x0191:		//	Ƒ	0xc6 0x91	LATIN CAPITAL LETTER F WITH HOOK
					*p = 0x0192;	//	ƒ	0xc6 0x92	LATIN SMALL LETTER F WITH HOOK
					break;
				case 0x0193:		//	Ɠ	0xc6 0x93	LATIN CAPITAL LETTER G WITH HOOK
					*p = 0x0260;	//	ɠ	0xc9 0xa0	LATIN SMALL LETTER G WITH HOOK
					break;
				case 0x0194:		//	Ɣ	0xc6 0x94	LATIN CAPITAL LETTER GAMMA
					*p = 0x0263;	//	ɣ	0xc9 0xa3	LATIN SMALL LETTER GAMMA
					break;
				case 0x0196:		//	Ɩ	0xc6 0x96	LATIN CAPITAL LETTER IOTA
					*p = 0x0269;	//	ɩ	0xc9 0xa9	LATIN SMALL LETTER IOTA
					break;
				case 0x0197:		//	Ɨ	0xc6 0x97	LATIN CAPITAL LETTER I WITH STROKE
					*p = 0x0268;	//	ɨ	0xc9 0xa8	LATIN SMALL LETTER I WITH STROKE
					break;
				case 0x0198:		//	Ƙ	0xc6 0x98	LATIN CAPITAL LETTER K WITH HOOK
					*p = 0x0199;	//	ƙ	0xc6 0x99	LATIN SMALL LETTER K WITH HOOK
					break;
				case 0x019C:		//	Ɯ	0xc6 0x9c	LATIN CAPITAL LETTER TURNED M
					*p = 0x026F;	//	ɯ	0xc9 0xaf	LATIN SMALL LETTER TURNED M
					break;
				case 0x019D:		//	Ɲ	0xc6 0x9d	LATIN CAPITAL LETTER N WITH LEFT HOOK
					*p = 0x0272;	//	ɲ	0xc9 0xb2	LATIN SMALL LETTER N WITH LEFT HOOK
					break;
				case 0x019F:		//	Ɵ	0xc6 0x9f	LATIN CAPITAL LETTER O WITH MIDDLE TILDE
					*p = 0x0275;	//	ɵ	0xc9 0xb5	LATIN SMALL LETTER O WITH MIDDLE TILDE
					break;
				case 0x01A0:		//	Ơ	0xc6 0xa0	LATIN CAPITAL LETTER O WITH HORN
					*p = 0x01A1;	//	ơ	0xc6 0xa1	LATIN SMALL LETTER O WITH HORN
					break;
				case 0x01A2:		//	Ƣ	0xc6 0xa2	LATIN CAPITAL LETTER OI
					*p = 0x01A3;	//	ƣ	0xc6 0xa3	LATIN SMALL LETTER OI
					break;
				case 0x01A4:		//	Ƥ	0xc6 0xa4	LATIN CAPITAL LETTER P WITH HOOK
					*p = 0x01A5;	//	ƥ	0xc6 0xa5	LATIN SMALL LETTER P WITH HOOK
					break;
				case 0x01A7:		//	Ƨ	0xc6 0xa7	LATIN CAPITAL LETTER TONE TWO
					*p = 0x01A8;	//	ƨ	0xc6 0xa8	LATIN SMALL LETTER TONE TWO
					break;
				case 0x01A9:		//	Ʃ	0xc6 0xa9	LATIN CAPITAL LETTER ESH
					*p = 0x0283;	//	ʃ	0xca 0x83	LATIN SMALL LETTER ESH
					break;
				case 0x01AC:		//	Ƭ	0xc6 0xac	LATIN CAPITAL LETTER T WITH HOOK
					*p = 0x01AD;	//	ƭ	0xc6 0xad	LATIN SMALL LETTER T WITH HOOK
					break;
				case 0x01AE:		//	Ʈ	0xc6 0xae	LATIN CAPITAL LETTER T WITH RETROFLEX HOOK
					*p = 0x0288;	//	ʈ	0xca 0x88	LATIN SMALL LETTER T WITH RETROFLEX HOOK
					break;
				case 0x01AF:		//	Ư	0xc6 0xaf	LATIN CAPITAL LETTER U WITH HORN
					*p = 0x01B0;	//	ư	0xc6 0xb0	LATIN SMALL LETTER U WITH HORN
					break;
				case 0x01B1:		//	Ʊ	0xc6 0xb1	LATIN CAPITAL LETTER UPSILON
					*p = 0x028A;	//	ʊ	0xca 0x8a	LATIN SMALL LETTER UPSILON
					break;
				case 0x01B2:		//	Ʋ	0xc6 0xb2	LATIN CAPITAL LETTER V WITH HOOK
					*p = 0x028B;	//	ʋ	0xca 0x8b	LATIN SMALL LETTER V WITH HOOK
					break;
				case 0x01B3:		//	Ƴ	0xc6 0xb3	LATIN CAPITAL LETTER Y WITH HOOK
					*p = 0x01B4;	//	ƴ	0xc6 0xb4	LATIN SMALL LETTER Y WITH HOOK
					break;
				case 0x01B5:		//	Ƶ	0xc6 0xb5	LATIN CAPITAL LETTER Z WITH STROKE
					*p = 0x01B6;	//	ƶ	0xc6 0xb6	LATIN SMALL LETTER Z WITH STROKE
					break;
				case 0x01B7:		//	Ʒ	0xc6 0xb7	LATIN CAPITAL LETTER EZH
					*p = 0x0292;	//	ʒ	0xca 0x92	LATIN SMALL LETTER EZH
					break;
				case 0x01B8:		//	Ƹ	0xc6 0xb8	LATIN CAPITAL LETTER EZH REVERSED
					*p = 0x01B9;	//	ƹ	0xc6 0xb9	LATIN SMALL LETTER EZH REVERSED
					break;
				case 0x01BC:		//	Ƽ	0xc6 0xbc	LATIN CAPITAL LETTER TONE FIVE
					*p = 0x01BD;	//	ƽ	0xc6 0xbd	LATIN SMALL LETTER TONE FIVE
					break;
				case 0x01C5:		//	ǅ	0xc7 0x85	LATIN CAPITAL LETTER D WITH SMALL Z WITH CARON
				case 0x01C4:		//	Ǆ	0xc7 0x84	LATIN CAPITAL LETTER DZ WITH CARON
					*p = 0x01C6;	//	ǆ	0xc7 0x86	LATIN SMALL LETTER DZ WITH CARON
					break;
				case 0x01C8:		//	ǈ	0xc7 0x88	LATIN CAPITAL LETTER L WITH SMALL J
				case 0x01C7:		//	Ǉ	0xc7 0x87	LATIN CAPITAL LETTER LJ
					*p = 0x01C9;	//	ǉ	0xc7 0x89	LATIN SMALL LETTER LJ
					break;
				case 0x01CB:		//	ǋ	0xc7 0x8b	LATIN CAPITAL LETTER N WITH SMALL J
				case 0x01CA:		//	Ǌ	0xc7 0x8a	LATIN CAPITAL LETTER NJ
					*p = 0x01CC;	//	ǌ	0xc7 0x8c	LATIN SMALL LETTER NJ
					break;
				case 0x01CD:		//	Ǎ	0xc7 0x8d	LATIN CAPITAL LETTER A WITH CARON
					*p = 0x01CE;	//	ǎ	0xc7 0x8e	LATIN SMALL LETTER A WITH CARON
					break;
				case 0x01CF:		//	Ǐ	0xc7 0x8f	LATIN CAPITAL LETTER I WITH CARON
					*p = 0x01D0;	//	ǐ	0xc7 0x90	LATIN SMALL LETTER I WITH CARON
					break;
				case 0x01D1:		//	Ǒ	0xc7 0x91	LATIN CAPITAL LETTER O WITH CARON
					*p = 0x01D2;	//	ǒ	0xc7 0x92	LATIN SMALL LETTER O WITH CARON
					break;
				case 0x01D3:		//	Ǔ	0xc7 0x93	LATIN CAPITAL LETTER U WITH CARON
					*p = 0x01D4;	//	ǔ	0xc7 0x94	LATIN SMALL LETTER U WITH CARON
					break;
				case 0x01D5:		//	Ǖ	0xc7 0x95	LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON
					*p = 0x01D6;	//	ǖ	0xc7 0x96	LATIN SMALL LETTER U WITH DIAERESIS AND MACRON
					break;
				case 0x01D7:		//	Ǘ	0xc7 0x97	LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE
					*p = 0x01D8;	//	ǘ	0xc7 0x98	LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE
					break;
				case 0x01D9:		//	Ǚ	0xc7 0x99	LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON
					*p = 0x01DA;	//	ǚ	0xc7 0x9a	LATIN SMALL LETTER U WITH DIAERESIS AND CARON
					break;
				case 0x01DB:		//	Ǜ	0xc7 0x9b	LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE
					*p = 0x01DC;	//	ǜ	0xc7 0x9c	LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE
					break;
				case 0x01DE:		//	Ǟ	0xc7 0x9e	LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON
					*p = 0x01DF;	//	ǟ	0xc7 0x9f	LATIN SMALL LETTER A WITH DIAERESIS AND MACRON
					break;
				case 0x01E0:		//	Ǡ	0xc7 0xa0	LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON
					*p = 0x01E1;	//	ǡ	0xc7 0xa1	LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON
					break;
				case 0x01E2:		//	Ǣ	0xc7 0xa2	LATIN CAPITAL LETTER AE WITH MACRON
					*p = 0x01E3;	//	ǣ	0xc7 0xa3	LATIN SMALL LETTER AE WITH MACRON
					break;
				case 0x01E4:		//	Ǥ	0xc7 0xa4	LATIN CAPITAL LETTER G WITH STROKE
					*p = 0x01E5;	//	ǥ	0xc7 0xa5	LATIN SMALL LETTER G WITH STROKE
					break;
				case 0x01E6:		//	Ǧ	0xc7 0xa6	LATIN CAPITAL LETTER G WITH CARON
					*p = 0x01E7;	//	ǧ	0xc7 0xa7	LATIN SMALL LETTER G WITH CARON
					break;
				case 0x01E8:		//	Ǩ	0xc7 0xa8	LATIN CAPITAL LETTER K WITH CARON
					*p = 0x01E9;	//	ǩ	0xc7 0xa9	LATIN SMALL LETTER K WITH CARON
					break;
				case 0x01EA:		//	Ǫ	0xc7 0xaa	LATIN CAPITAL LETTER O WITH OGONEK
					*p = 0x01EB;	//	ǫ	0xc7 0xab	LATIN SMALL LETTER O WITH OGONEK
					break;
				case 0x01EC:		//	Ǭ	0xc7 0xac	LATIN CAPITAL LETTER O WITH OGONEK AND MACRON
					*p = 0x01ED;	//	ǭ	0xc7 0xad	LATIN SMALL LETTER O WITH OGONEK AND MACRON
					break;
				case 0x01EE:		//	Ǯ	0xc7 0xae	LATIN CAPITAL LETTER EZH WITH CARON
					*p = 0x01EF;	//	ǯ	0xc7 0xaf	LATIN SMALL LETTER EZH WITH CARON
					break;
				case 0x01F1:		//	Ǳ	0xc7 0xb1	LATIN CAPITAL LETTER DZ
				case 0x01F2:		//	ǲ	0xc7 0xb2	LATIN CAPITAL LETTER D WITH SMALL Z
					*p = 0x01F3;	//	ǳ	0xc7 0xb3	LATIN SMALL LETTER DZ
					break;
				case 0x01F4:		//	Ǵ	0xc7 0xb4	LATIN CAPITAL LETTER G WITH ACUTE
					*p = 0x01F5;	//	ǵ	0xc7 0xb5	LATIN SMALL LETTER G WITH ACUTE
					break;
				case 0x01F6:		//	Ƕ	0xc7 0xb6	LATIN CAPITAL LETTER HWAIR
					*p = 0x0195;	//	ƕ	0xc6 0x95	LATIN SMALL LETTER HWAIR
					break;
				case 0x01F7:		//	Ƿ	0xc7 0xb7	LATIN CAPITAL LETTER WYNN
					*p = 0x01BF;	//	ƿ	0xc6 0xbf	LATIN SMALL LETTER WYNN
					break;
				case 0x01F8:		//	Ǹ	0xc7 0xb8	LATIN CAPITAL LETTER N WITH GRAVE
					*p = 0x01F9;	//	ǹ	0xc7 0xb9	LATIN SMALL LETTER N WITH GRAVE
					break;
				case 0x01FA:		//	Ǻ	0xc7 0xba	LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE
					*p = 0x01FB;	//	ǻ	0xc7 0xbb	LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE
					break;
				case 0x01FC:		//	Ǽ	0xc7 0xbc	LATIN CAPITAL LETTER AE WITH ACUTE
					*p = 0x01FD;	//	ǽ	0xc7 0xbd	LATIN SMALL LETTER AE WITH ACUTE
					break;
				case 0x01FE:		//	Ǿ	0xc7 0xbe	LATIN CAPITAL LETTER O WITH STROKE AND ACUTE
					*p = 0x01FF;	//	ǿ	0xc7 0xbf	LATIN SMALL LETTER O WITH STROKE AND ACUTE
					break;
				case 0x0200:		//	Ȁ	0xc8 0x80	LATIN CAPITAL LETTER A WITH DOUBLE GRAVE
					*p = 0x0201;	//	ȁ	0xc8 0x81	LATIN SMALL LETTER A WITH DOUBLE GRAVE
					break;
				case 0x0202:		//	Ȃ	0xc8 0x82	LATIN CAPITAL LETTER A WITH INVERTED BREVE
					*p = 0x0203;	//	ȃ	0xc8 0x83	LATIN SMALL LETTER A WITH INVERTED BREVE
					break;
				case 0x0204:		//	Ȅ	0xc8 0x84	LATIN CAPITAL LETTER E WITH DOUBLE GRAVE
					*p = 0x0205;	//	ȅ	0xc8 0x85	LATIN SMALL LETTER E WITH DOUBLE GRAVE
					break;
				case 0x0206:		//	Ȇ	0xc8 0x86	LATIN CAPITAL LETTER E WITH INVERTED BREVE
					*p = 0x0207;	//	ȇ	0xc8 0x87	LATIN SMALL LETTER E WITH INVERTED BREVE
					break;
				case 0x0208:		//	Ȉ	0xc8 0x88	LATIN CAPITAL LETTER I WITH DOUBLE GRAVE
					*p = 0x0209;	//	ȉ	0xc8 0x89	LATIN SMALL LETTER I WITH DOUBLE GRAVE
					break;
				case 0x020A:		//	Ȋ	0xc8 0x8a	LATIN CAPITAL LETTER I WITH INVERTED BREVE
					*p = 0x020B;	//	ȋ	0xc8 0x8b	LATIN SMALL LETTER I WITH INVERTED BREVE
					break;
				case 0x020C:		//	Ȍ	0xc8 0x8c	LATIN CAPITAL LETTER O WITH DOUBLE GRAVE
					*p = 0x020D;	//	ȍ	0xc8 0x8d	LATIN SMALL LETTER O WITH DOUBLE GRAVE
					break;
				case 0x020E:		//	Ȏ	0xc8 0x8e	LATIN CAPITAL LETTER O WITH INVERTED BREVE
					*p = 0x020F;	//	ȏ	0xc8 0x8f	LATIN SMALL LETTER O WITH INVERTED BREVE
					break;
				case 0x0210:		//	Ȑ	0xc8 0x90	LATIN CAPITAL LETTER R WITH DOUBLE GRAVE
					*p = 0x0211;	//	ȑ	0xc8 0x91	LATIN SMALL LETTER R WITH DOUBLE GRAVE
					break;
				case 0x0212:		//	Ȓ	0xc8 0x92	LATIN CAPITAL LETTER R WITH INVERTED BREVE
					*p = 0x0213;	//	ȓ	0xc8 0x93	LATIN SMALL LETTER R WITH INVERTED BREVE
					break;
				case 0x0214:		//	Ȕ	0xc8 0x94	LATIN CAPITAL LETTER U WITH DOUBLE GRAVE
					*p = 0x0215;	//	ȕ	0xc8 0x95	LATIN SMALL LETTER U WITH DOUBLE GRAVE
					break;
				case 0x0216:		//	Ȗ	0xc8 0x96	LATIN CAPITAL LETTER U WITH INVERTED BREVE
					*p = 0x0217;	//	ȗ	0xc8 0x97	LATIN SMALL LETTER U WITH INVERTED BREVE
					break;
				case 0x0218:		//	Ș	0xc8 0x98	LATIN CAPITAL LETTER S WITH COMMA BELOW
					*p = 0x0219;	//	ș	0xc8 0x99	LATIN SMALL LETTER S WITH COMMA BELOW
					break;
				case 0x021A:		//	Ț	0xc8 0x9a	LATIN CAPITAL LETTER T WITH COMMA BELOW
					*p = 0x021B;	//	ț	0xc8 0x9b	LATIN SMALL LETTER T WITH COMMA BELOW
					break;
				case 0x021C:		//	Ȝ	0xc8 0x9c	LATIN CAPITAL LETTER YOGH
					*p = 0x021D;	//	ȝ	0xc8 0x9d	LATIN SMALL LETTER YOGH
					break;
				case 0x021E:		//	Ȟ	0xc8 0x9e	LATIN CAPITAL LETTER H WITH CARON
					*p = 0x021F;	//	ȟ	0xc8 0x9f	LATIN SMALL LETTER H WITH CARON
					break;
				case 0x0220:		//	Ƞ	0xc8 0xa0	LATIN CAPITAL LETTER N WITH LONG RIGHT LEG
					*p = 0x019E;	//	ƞ	0xc6 0x9e	LATIN SMALL LETTER N WITH LONG RIGHT LEG
					break;
				case 0x0222:		//	Ȣ	0xc8 0xa2	LATIN CAPITAL LETTER OU
					*p = 0x0223;	//	ȣ	0xc8 0xa3	LATIN SMALL LETTER OU
					break;
				case 0x0224:		//	Ȥ	0xc8 0xa4	LATIN CAPITAL LETTER Z WITH HOOK
					*p = 0x0225;	//	ȥ	0xc8 0xa5	LATIN SMALL LETTER Z WITH HOOK
					break;
				case 0x0226:		//	Ȧ	0xc8 0xa6	LATIN CAPITAL LETTER A WITH DOT ABOVE
					*p = 0x0227;	//	ȧ	0xc8 0xa7	LATIN SMALL LETTER A WITH DOT ABOVE
					break;
				case 0x0228:		//	Ȩ	0xc8 0xa8	LATIN CAPITAL LETTER E WITH CEDILLA
					*p = 0x0229;	//	ȩ	0xc8 0xa9	LATIN SMALL LETTER E WITH CEDILLA
					break;
				case 0x022A:		//	Ȫ	0xc8 0xaa	LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON
					*p = 0x022B;	//	ȫ	0xc8 0xab	LATIN SMALL LETTER O WITH DIAERESIS AND MACRON
					break;
				case 0x022C:		//	Ȭ	0xc8 0xac	LATIN CAPITAL LETTER O WITH TILDE AND MACRON
					*p = 0x022D;	//	ȭ	0xc8 0xad	LATIN SMALL LETTER O WITH TILDE AND MACRON
					break;
				case 0x022E:		//	Ȯ	0xc8 0xae	LATIN CAPITAL LETTER O WITH DOT ABOVE
					*p = 0x022F;	//	ȯ	0xc8 0xaf	LATIN SMALL LETTER O WITH DOT ABOVE
					break;
				case 0x0230:		//	Ȱ	0xc8 0xb0	LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON
					*p = 0x0231;	//	ȱ	0xc8 0xb1	LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON
					break;
				case 0x0232:		//	Ȳ	0xc8 0xb2	LATIN CAPITAL LETTER Y WITH MACRON
					*p = 0x0233;	//	ȳ	0xc8 0xb3	LATIN SMALL LETTER Y WITH MACRON
					break;
				case 0x023A:		//	Ⱥ	0xc8 0xba	LATIN CAPITAL LETTER A WITH STROKE
					*p = 0x2C65;	//	ⱥ	0xe2 0xb1 0xa5	LATIN SMALL LETTER A WITH STROKE
					break;
				case 0x023B:		//	Ȼ	0xc8 0xbb	LATIN CAPITAL LETTER C WITH STROKE
					*p = 0x023C;	//	ȼ	0xc8 0xbc	LATIN SMALL LETTER C WITH STROKE
					break;
				case 0x023D:		//	Ƚ	0xc8 0xbd	LATIN CAPITAL LETTER L WITH BAR
					*p = 0x019A;	//	ƚ	0xc6 0x9a	LATIN SMALL LETTER L WITH BAR
					break;
				case 0x023E:		//	Ⱦ	0xc8 0xbe	LATIN CAPITAL LETTER T WITH DIAGONAL STROKE
					*p = 0x2C66;	//	ⱦ	0xe2 0xb1 0xa6	LATIN SMALL LETTER T WITH DIAGONAL STROKE
					break;
				case 0x0241:		//	Ɂ	0xc9 0x81	LATIN CAPITAL LETTER GLOTTAL STOP
					*p = 0x0242;	//	ɂ	0xc9 0x82	LATIN SMALL LETTER GLOTTAL STOP
					break;
				case 0x0243:		//	Ƀ	0xc9 0x83	LATIN CAPITAL LETTER B WITH STROKE
					*p = 0x0180;	//	ƀ	0xc6 0x80	LATIN SMALL LETTER B WITH STROKE
					break;
				case 0x0244:		//	Ʉ	0xc9 0x84	LATIN CAPITAL LETTER U BAR
					*p = 0x0289;	//	ʉ	0xca 0x89	LATIN SMALL LETTER U BAR
					break;
				case 0x0245:		//	Ʌ	0xc9 0x85	LATIN CAPITAL LETTER TURNED V
					*p = 0x028C;	//	ʌ	0xca 0x8c	LATIN SMALL LETTER TURNED V
					break;
				case 0x0246:		//	Ɇ	0xc9 0x86	LATIN CAPITAL LETTER E WITH STROKE
					*p = 0x0247;	//	ɇ	0xc9 0x87	LATIN SMALL LETTER E WITH STROKE
					break;
				case 0x0248:		//	Ɉ	0xc9 0x88	LATIN CAPITAL LETTER J WITH STROKE
					*p = 0x0249;	//	ɉ	0xc9 0x89	LATIN SMALL LETTER J WITH STROKE
					break;
				case 0x024A:		//	Ɋ	0xc9 0x8a	LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL
					*p = 0x024B;	//	ɋ	0xc9 0x8b	LATIN SMALL LETTER SMALL Q WITH HOOK TAIL
					break;
				case 0x024C:		//	Ɍ	0xc9 0x8c	LATIN CAPITAL LETTER R WITH STROKE
					*p = 0x024D;	//	ɍ	0xc9 0x8d	LATIN SMALL LETTER R WITH STROKE
					break;
				case 0x024E:		//	Ɏ	0xc9 0x8e	LATIN CAPITAL LETTER Y WITH STROKE
					*p = 0x024F;	//	ɏ	0xc9 0x8f	LATIN SMALL LETTER Y WITH STROKE
					break;
				case 0x0370:		//	Ͱ	0xcd 0xb0	GREEK CAPITAL LETTER HETA
					*p = 0x0371;	//	ͱ	0xcd 0xb1	GREEK SMALL LETTER HETA
					break;
				case 0x0372:		//	Ͳ	0xcd 0xb2	GREEK CAPITAL LETTER ARCHAIC SAMPI
					*p = 0x0373;	//	ͳ	0xcd 0xb3	GREEK SMALL LETTER ARCHAIC SAMPI
					break;
				case 0x0376:		//	Ͷ	0xcd 0xb6	GREEK CAPITAL LETTER PAMPHYLIAN DIGAMMA
					*p = 0x0377;	//	ͷ	0xcd 0xb7	GREEK SMALL LETTER PAMPHYLIAN DIGAMMA
					break;
				case 0x037F:		//	Ϳ	0xcd 0xbf	GREEK CAPITAL LETTER YOT
					*p = 0x03F3;	//	ϳ	0xcf 0xb3	GREEK SMALL LETTER YOT
					break;
				case 0x0386:		//	Ά	0xce 0x86	GREEK CAPITAL LETTER ALPHA WITH TONOS
					*p = 0x03AC;	//	ά	0xce 0xac	GREEK SMALL LETTER ALPHA WITH TONOS
					break;
				case 0x0388:		//	Έ	0xce 0x88	GREEK CAPITAL LETTER EPSILON WITH TONOS
					*p = 0x03AD;	//	έ	0xce 0xad	GREEK SMALL LETTER EPSILON WITH TONOS
					break;
				case 0x0389:		//	Ή	0xce 0x89	GREEK CAPITAL LETTER ETA WITH TONOS
					*p = 0x03AE;	//	ή	0xce 0xae	GREEK SMALL LETTER ETA WITH TONOS
					break;
				case 0x038A:		//	Ί	0xce 0x8a	GREEK CAPITAL LETTER IOTA WITH TONOS
					*p = 0x03AF;	//	ί	0xce 0xaf	GREEK SMALL LETTER IOTA WITH TONOS
					break;
				case 0x038C:		//	Ό	0xce 0x8c	GREEK CAPITAL LETTER OMICRON WITH TONOS
					*p = 0x03CC;	//	ό	0xcf 0x8c	GREEK SMALL LETTER OMICRON WITH TONOS
					break;
				case 0x038E:		//	Ύ	0xce 0x8e	GREEK CAPITAL LETTER UPSILON WITH TONOS
					*p = 0x03CD;	//	ύ	0xcf 0x8d	GREEK SMALL LETTER UPSILON WITH TONOS
					break;
				case 0x038F:		//	Ώ	0xce 0x8f	GREEK CAPITAL LETTER OMEGA WITH TONOS
					*p = 0x03CE;	//	ώ	0xcf 0x8e	GREEK SMALL LETTER OMEGA WITH TONOS
					break;
				case 0x0391:		//	Α	0xce 0x91	GREEK CAPITAL LETTER ALPHA
					*p = 0x03B1;	//	α	0xce 0xb1	GREEK SMALL LETTER ALPHA
					break;
				case 0x0392:		//	Β	0xce 0x92	GREEK CAPITAL LETTER BETA
					*p = 0x03B2;	//	β	0xce 0xb2	GREEK SMALL LETTER BETA
					break;
				case 0x0393:		//	Γ	0xce 0x93	GREEK CAPITAL LETTER GAMMA
					*p = 0x03B3;	//	γ	0xce 0xb3	GREEK SMALL LETTER GAMMA
					break;
				case 0x0394:		//	Δ	0xce 0x94	GREEK CAPITAL LETTER DELTA
					*p = 0x03B4;	//	δ	0xce 0xb4	GREEK SMALL LETTER DELTA
					break;
				case 0x0395:		//	Ε	0xce 0x95	GREEK CAPITAL LETTER EPSILON
					*p = 0x03B5;	//	ε	0xce 0xb5	GREEK SMALL LETTER EPSILON
					break;
				case 0x0396:		//	Ζ	0xce 0x96	GREEK CAPITAL LETTER ZETA
					*p = 0x03B6;	//	ζ	0xce 0xb6	GREEK SMALL LETTER ZETA
					break;
				case 0x0397:		//	Η	0xce 0x97	GREEK CAPITAL LETTER ETA
					*p = 0x03B7;	//	η	0xce 0xb7	GREEK SMALL LETTER ETA
					break;
				case 0x0398:		//	Θ	0xce 0x98	GREEK CAPITAL LETTER THETA
					*p = 0x03B8;	//	θ	0xce 0xb8	GREEK SMALL LETTER THETA
					break;
				case 0x0399:		//	Ι	0xce 0x99	GREEK CAPITAL LETTER IOTA
					*p = 0x03B9;	//	ι	0xce 0xb9	GREEK SMALL LETTER IOTA
					break;
				case 0x039A:		//	Κ	0xce 0x9a	GREEK CAPITAL LETTER KAPPA
					*p = 0x03BA;	//	κ	0xce 0xba	GREEK SMALL LETTER KAPPA
					break;
				case 0x039B:		//	Λ	0xce 0x9b	GREEK CAPITAL LETTER LAMDA
					*p = 0x03BB;	//	λ	0xce 0xbb	GREEK SMALL LETTER LAMDA
					break;
				case 0x039C:		//	Μ	0xce 0x9c	GREEK CAPITAL LETTER MU
					*p = 0x03BC;	//	μ	0xce 0xbc	GREEK SMALL LETTER MU
					break;
				case 0x039D:		//	Ν	0xce 0x9d	GREEK CAPITAL LETTER NU
					*p = 0x03BD;	//	ν	0xce 0xbd	GREEK SMALL LETTER NU
					break;
				case 0x039E:		//	Ξ	0xce 0x9e	GREEK CAPITAL LETTER XI
					*p = 0x03BE;	//	ξ	0xce 0xbe	GREEK SMALL LETTER XI
					break;
				case 0x039F:		//	Ο	0xce 0x9f	GREEK CAPITAL LETTER OMICRON
					*p = 0x03BF;	//	ο	0xce 0xbf	GREEK SMALL LETTER OMICRON
					break;
				case 0x03A0:		//	Π	0xce 0xa0	GREEK CAPITAL LETTER PI
					*p = 0x03C0;	//	π	0xcf 0x80	GREEK SMALL LETTER PI
					break;
				case 0x03A1:		//	Ρ	0xce 0xa1	GREEK CAPITAL LETTER RHO
					*p = 0x03C1;	//	ρ	0xcf 0x81	GREEK SMALL LETTER RHO
					break;
				case 0x03C2:		//	ς	0xcf 0x82	GREEK SMALL FINAL SIGMA // To handle sigma as the same character in search
				case 0x03A3:		//	Σ	0xce 0xa3	GREEK CAPITAL LETTER SIGMA
					*p = 0x03C3;	//	σ	0xcf 0x83	GREEK SMALL LETTER SIGMA
					break;
				case 0x03A4:		//	Τ	0xce 0xa4	GREEK CAPITAL LETTER TAU
					*p = 0x03C4;	//	τ	0xcf 0x84	GREEK SMALL LETTER TAU
					break;
				case 0x03A5:		//	Υ	0xce 0xa5	GREEK CAPITAL LETTER UPSILON
					*p = 0x03C5;	//	υ	0xcf 0x85	GREEK SMALL LETTER UPSILON
					break;
				case 0x03A6:		//	Φ	0xce 0xa6	GREEK CAPITAL LETTER PHI
					*p = 0x03C6;	//	φ	0xcf 0x86	GREEK SMALL LETTER PHI
					break;
				case 0x03A7:		//	Χ	0xce 0xa7	GREEK CAPITAL LETTER CHI
					*p = 0x03C7;	//	χ	0xcf 0x87	GREEK SMALL LETTER CHI
					break;
				case 0x03A8:		//	Ψ	0xce 0xa8	GREEK CAPITAL LETTER PSI
					*p = 0x03C8;	//	ψ	0xcf 0x88	GREEK SMALL LETTER PSI
					break;
				case 0x03A9:		//	Ω	0xce 0xa9	GREEK CAPITAL LETTER OMEGA
					*p = 0x03C9;	//	ω	0xcf 0x89	GREEK SMALL LETTER OMEGA
					break;
				case 0x03AA:		//	Ϊ	0xce 0xaa	GREEK CAPITAL LETTER IOTA WITH DIALYTIKA
					*p = 0x03CA;	//	ϊ	0xcf 0x8a	GREEK SMALL LETTER IOTA WITH DIALYTIKA
					break;
				case 0x03AB:		//	Ϋ	0xce 0xab	GREEK CAPITAL LETTER UPSILON WITH DIALYTIKA
					*p = 0x03CB;	//	ϋ	0xcf 0x8b	GREEK SMALL LETTER UPSILON WITH DIALYTIKA
					break;
				case 0x03CF:		//	Ϗ	0xcf 0x8f	GREEK CAPITAL LETTER KAI SYMBOL
					*p = 0x03D7;	//	ϗ	0xcf 0x97	GREEK SMALL LETTER KAI SYMBOL
					break;
				case 0x03D8:		//	Ϙ	0xcf 0x98	GREEK CAPITAL LETTER ARCHAIC KOPPA
					*p = 0x03D9;	//	ϙ	0xcf 0x99	GREEK SMALL LETTER ARCHAIC KOPPA
					break;
				case 0x03DA:		//	Ϛ	0xcf 0x9a	GREEK CAPITAL LETTER STIGMA
					*p = 0x03DB;	//	ϛ	0xcf 0x9b	GREEK SMALL LETTER STIGMA
					break;
				case 0x03DC:		//	Ϝ	0xcf 0x9c	GREEK CAPITAL LETTER DIGAMMA
					*p = 0x03DD;	//	ϝ	0xcf 0x9d	GREEK SMALL LETTER DIGAMMA
					break;
				case 0x03DE:		//	Ϟ	0xcf 0x9e	GREEK CAPITAL LETTER KOPPA
					*p = 0x03DF;	//	ϟ	0xcf 0x9f	GREEK SMALL LETTER KOPPA
					break;
				case 0x03E0:		//	Ϡ	0xcf 0xa0	GREEK CAPITAL LETTER SAMPI
					*p = 0x03E1;	//	ϡ	0xcf 0xa1	GREEK SMALL LETTER SAMPI
					break;
				case 0x03E2:		//	Ϣ	0xcf 0xa2	COPTIC CAPITAL LETTER SHEI
					*p = 0x03E3;	//	ϣ	0xcf 0xa3	COPTIC SMALL LETTER SHEI
					break;
				case 0x03E4:		//	Ϥ	0xcf 0xa4	COPTIC CAPITAL LETTER FEI
					*p = 0x03E5;	//	ϥ	0xcf 0xa5	COPTIC SMALL LETTER FEI
					break;
				case 0x03E6:		//	Ϧ	0xcf 0xa6	COPTIC CAPITAL LETTER KHEI
					*p = 0x03E7;	//	ϧ	0xcf 0xa7	COPTIC SMALL LETTER KHEI
					break;
				case 0x03E8:		//	Ϩ	0xcf 0xa8	COPTIC CAPITAL LETTER HORI
					*p = 0x03E9;	//	ϩ	0xcf 0xa9	COPTIC SMALL LETTER HORI
					break;
				case 0x03EA:		//	Ϫ	0xcf 0xaa	COPTIC CAPITAL LETTER GANGIA
					*p = 0x03EB;	//	ϫ	0xcf 0xab	COPTIC SMALL LETTER GANGIA
					break;
				case 0x03EC:		//	Ϭ	0xcf 0xac	COPTIC CAPITAL LETTER SHIMA
					*p = 0x03ED;	//	ϭ	0xcf 0xad	COPTIC SMALL LETTER SHIMA
					break;
				case 0x03EE:		//	Ϯ	0xcf 0xae	COPTIC CAPITAL LETTER DEI
					*p = 0x03EF;	//	ϯ	0xcf 0xaf	COPTIC SMALL LETTER DEI
					break;
				case 0x03F4:		//	ϴ	0xcf 0xb4	GREEK CAPITAL LETTER THETA SYMBOL
					*p = 0x03D1;	//	ϑ	0xcf 0x91	GREEK SMALL LETTER THETA SYMBOL
					break;
				case 0x03F7:		//	Ϸ	0xcf 0xb7	GREEK CAPITAL LETTER SHO
					*p = 0x03F8;	//	ϸ	0xcf 0xb8	GREEK SMALL LETTER SHO
					break;
				case 0x03F9:		//	Ϲ	0xcf 0xb9	GREEK CAPITAL LETTER LUNATE SIGMA SYMBOL
					*p = 0x03F2;	//	ϲ	0xcf 0xb2	GREEK SMALL LETTER LUNATE SIGMA SYMBOL
					break;
				case 0x03FA:		//	Ϻ	0xcf 0xba	GREEK CAPITAL LETTER SAN
					*p = 0x03FB;	//	ϻ	0xcf 0xbb	GREEK SMALL LETTER SAN
					break;
				case 0x03FD:		//	Ͻ	0xcf 0xbd	GREEK CAPITAL LETTER REVERSED LUNATE SIGMA SYMBOL
					*p = 0x037B;	//	ͻ	0xcd 0xbb	GREEK SMALL LETTER REVERSED LUNATE SIGMA SYMBOL
					break;
				case 0x03FE:		//	Ͼ	0xcf 0xbe	GREEK CAPITAL LETTER DOTTED LUNATE SIGMA SYMBOL
					*p = 0x037C;	//	ͼ	0xcd 0xbc	GREEK SMALL LETTER DOTTED LUNATE SIGMA SYMBOL
					break;
				case 0x03FF:		//	Ͽ	0xcf 0xbf	GREEK CAPITAL LETTER REVERSED DOTTED LUNATE SIGMA SYMBOL
					*p = 0x037D;	//	ͽ	0xcd 0xbd	GREEK SMALL LETTER REVERSED DOTTED LUNATE SIGMA SYMBOL
					break;
				case 0x0400:		//	Ѐ	0xd0 0x80	CYRILLIC CAPITAL LETTER IE WITH GRAVE
					*p = 0x0450;	//	ѐ	0xd1 0x90	CYRILLIC SMALL LETTER IE WITH GRAVE
					break;
				case 0x0401:		//	Ё	0xd0 0x81	CYRILLIC CAPITAL LETTER IO
					*p = 0x0451;	//	ё	0xd1 0x91	CYRILLIC SMALL LETTER IO
					break;
				case 0x0402:		//	Ђ	0xd0 0x82	CYRILLIC CAPITAL LETTER DJE
					*p = 0x0452;	//	ђ	0xd1 0x92	CYRILLIC SMALL LETTER DJE
					break;
				case 0x0403:		//	Ѓ	0xd0 0x83	CYRILLIC CAPITAL LETTER GJE
					*p = 0x0453;	//	ѓ	0xd1 0x93	CYRILLIC SMALL LETTER GJE
					break;
				case 0x0404:		//	Є	0xd0 0x84	CYRILLIC CAPITAL LETTER UKRAINIAN IE
					*p = 0x0454;	//	є	0xd1 0x94	CYRILLIC SMALL LETTER UKRAINIAN IE
					break;
				case 0x0405:		//	Ѕ	0xd0 0x85	CYRILLIC CAPITAL LETTER DZE
					*p = 0x0455;	//	ѕ	0xd1 0x95	CYRILLIC SMALL LETTER DZE
					break;
				case 0x0406:		//	І	0xd0 0x86	CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I
					*p = 0x0456;	//	і	0xd1 0x96	CYRILLIC SMALL LETTER BYELORUSSIAN-UKRAINIAN I
					break;
				case 0x0407:		//	Ї	0xd0 0x87	CYRILLIC CAPITAL LETTER YI
					*p = 0x0457;	//	ї	0xd1 0x97	CYRILLIC SMALL LETTER YI
					break;
				case 0x0408:		//	Ј	0xd0 0x88	CYRILLIC CAPITAL LETTER JE
					*p = 0x0458;	//	ј	0xd1 0x98	CYRILLIC SMALL LETTER JE
					break;
				case 0x0409:		//	Љ	0xd0 0x89	CYRILLIC CAPITAL LETTER LJE
					*p = 0x0459;	//	љ	0xd1 0x99	CYRILLIC SMALL LETTER LJE
					break;
				case 0x040A:		//	Њ	0xd0 0x8a	CYRILLIC CAPITAL LETTER NJE
					*p = 0x045A;	//	њ	0xd1 0x9a	CYRILLIC SMALL LETTER NJE
					break;
				case 0x040B:		//	Ћ	0xd0 0x8b	CYRILLIC CAPITAL LETTER TSHE
					*p = 0x045B;	//	ћ	0xd1 0x9b	CYRILLIC SMALL LETTER TSHE
					break;
				case 0x040C:		//	Ќ	0xd0 0x8c	CYRILLIC CAPITAL LETTER KJE
					*p = 0x045C;	//	ќ	0xd1 0x9c	CYRILLIC SMALL LETTER KJE
					break;
				case 0x040D:		//	Ѝ	0xd0 0x8d	CYRILLIC CAPITAL LETTER I WITH GRAVE
					*p = 0x045D;	//	ѝ	0xd1 0x9d	CYRILLIC SMALL LETTER I WITH GRAVE
					break;
				case 0x040E:		//	Ў	0xd0 0x8e	CYRILLIC CAPITAL LETTER SHORT U
					*p = 0x045E;	//	ў	0xd1 0x9e	CYRILLIC SMALL LETTER SHORT U
					break;
				case 0x040F:		//	Џ	0xd0 0x8f	CYRILLIC CAPITAL LETTER DZHE
					*p = 0x045F;	//	џ	0xd1 0x9f	CYRILLIC SMALL LETTER DZHE
					break;
				case 0x0410:		//	А	0xd0 0x90	CYRILLIC CAPITAL LETTER A
					*p = 0x0430;	//	а	0xd0 0xb0	CYRILLIC SMALL LETTER A
					break;
				case 0x0411:		//	Б	0xd0 0x91	CYRILLIC CAPITAL LETTER BE
					*p = 0x0431;	//	б	0xd0 0xb1	CYRILLIC SMALL LETTER BE
					break;
				case 0x0412:		//	В	0xd0 0x92	CYRILLIC CAPITAL LETTER VE
					*p = 0x0432;	//	в	0xd0 0xb2	CYRILLIC SMALL LETTER VE
					break;
				case 0x0413:		//	Г	0xd0 0x93	CYRILLIC CAPITAL LETTER GHE
					*p = 0x0433;	//	г	0xd0 0xb3	CYRILLIC SMALL LETTER GHE
					break;
				case 0x0414:		//	Д	0xd0 0x94	CYRILLIC CAPITAL LETTER DE
					*p = 0x0434;	//	д	0xd0 0xb4	CYRILLIC SMALL LETTER DE
					break;
				case 0x0415:		//	Е	0xd0 0x95	CYRILLIC CAPITAL LETTER IE
					*p = 0x0435;	//	е	0xd0 0xb5	CYRILLIC SMALL LETTER IE
					break;
				case 0x0416:		//	Ж	0xd0 0x96	CYRILLIC CAPITAL LETTER ZHE
					*p = 0x0436;	//	ж	0xd0 0xb6	CYRILLIC SMALL LETTER ZHE
					break;
				case 0x0417:		//	З	0xd0 0x97	CYRILLIC CAPITAL LETTER ZE
					*p = 0x0437;	//	з	0xd0 0xb7	CYRILLIC SMALL LETTER ZE
					break;
				case 0x0418:		//	И	0xd0 0x98	CYRILLIC CAPITAL LETTER I
					*p = 0x0438;	//	и	0xd0 0xb8	CYRILLIC SMALL LETTER I
					break;
				case 0x0419:		//	Й	0xd0 0x99	CYRILLIC CAPITAL LETTER SHORT I
					*p = 0x0439;	//	й	0xd0 0xb9	CYRILLIC SMALL LETTER SHORT I
					break;
				case 0x041A:		//	К	0xd0 0x9a	CYRILLIC CAPITAL LETTER KA
					*p = 0x043A;	//	к	0xd0 0xba	CYRILLIC SMALL LETTER KA
					break;
				case 0x041B:		//	Л	0xd0 0x9b	CYRILLIC CAPITAL LETTER EL
					*p = 0x043B;	//	л	0xd0 0xbb	CYRILLIC SMALL LETTER EL
					break;
				case 0x041C:		//	М	0xd0 0x9c	CYRILLIC CAPITAL LETTER EM
					*p = 0x043C;	//	м	0xd0 0xbc	CYRILLIC SMALL LETTER EM
					break;
				case 0x041D:		//	Н	0xd0 0x9d	CYRILLIC CAPITAL LETTER EN
					*p = 0x043D;	//	н	0xd0 0xbd	CYRILLIC SMALL LETTER EN
					break;
				case 0x041E:		//	О	0xd0 0x9e	CYRILLIC CAPITAL LETTER O
					*p = 0x043E;	//	о	0xd0 0xbe	CYRILLIC SMALL LETTER O
					break;
				case 0x041F:		//	П	0xd0 0x9f	CYRILLIC CAPITAL LETTER PE
					*p = 0x043F;	//	п	0xd0 0xbf	CYRILLIC SMALL LETTER PE
					break;
				case 0x0420:		//	Р	0xd0 0xa0	CYRILLIC CAPITAL LETTER ER
					*p = 0x0440;	//	р	0xd1 0x80	CYRILLIC SMALL LETTER ER
					break;
				case 0x0421:		//	С	0xd0 0xa1	CYRILLIC CAPITAL LETTER ES
					*p = 0x0441;	//	с	0xd1 0x81	CYRILLIC SMALL LETTER ES
					break;
				case 0x0422:		//	Т	0xd0 0xa2	CYRILLIC CAPITAL LETTER TE
					*p = 0x0442;	//	т	0xd1 0x82	CYRILLIC SMALL LETTER TE
					break;
				case 0x0423:		//	У	0xd0 0xa3	CYRILLIC CAPITAL LETTER U
					*p = 0x0443;	//	у	0xd1 0x83	CYRILLIC SMALL LETTER U
					break;
				case 0x0424:		//	Ф	0xd0 0xa4	CYRILLIC CAPITAL LETTER EF
					*p = 0x0444;	//	ф	0xd1 0x84	CYRILLIC SMALL LETTER EF
					break;
				case 0x0425:		//	Х	0xd0 0xa5	CYRILLIC CAPITAL LETTER HA
					*p = 0x0445;	//	х	0xd1 0x85	CYRILLIC SMALL LETTER HA
					break;
				case 0x0426:		//	Ц	0xd0 0xa6	CYRILLIC CAPITAL LETTER TSE
					*p = 0x0446;	//	ц	0xd1 0x86	CYRILLIC SMALL LETTER TSE
					break;
				case 0x0427:		//	Ч	0xd0 0xa7	CYRILLIC CAPITAL LETTER CHE
					*p = 0x0447;	//	ч	0xd1 0x87	CYRILLIC SMALL LETTER CHE
					break;
				case 0x0428:		//	Ш	0xd0 0xa8	CYRILLIC CAPITAL LETTER SHA
					*p = 0x0448;	//	ш	0xd1 0x88	CYRILLIC SMALL LETTER SHA
					break;
				case 0x0429:		//	Щ	0xd0 0xa9	CYRILLIC CAPITAL LETTER SHCHA
					*p = 0x0449;	//	щ	0xd1 0x89	CYRILLIC SMALL LETTER SHCHA
					break;
				case 0x042A:		//	Ъ	0xd0 0xaa	CYRILLIC CAPITAL LETTER HARD SIGN
					*p = 0x044A;	//	ъ	0xd1 0x8a	CYRILLIC SMALL LETTER HARD SIGN
					break;
				case 0x042B:		//	Ы	0xd0 0xab	CYRILLIC CAPITAL LETTER YERU
					*p = 0x044B;	//	ы	0xd1 0x8b	CYRILLIC SMALL LETTER YERU
					break;
				case 0x042C:		//	Ь	0xd0 0xac	CYRILLIC CAPITAL LETTER SOFT SIGN
					*p = 0x044C;	//	ь	0xd1 0x8c	CYRILLIC SMALL LETTER SOFT SIGN
					break;
				case 0x042D:		//	Э	0xd0 0xad	CYRILLIC CAPITAL LETTER E
					*p = 0x044D;	//	э	0xd1 0x8d	CYRILLIC SMALL LETTER E
					break;
				case 0x042E:		//	Ю	0xd0 0xae	CYRILLIC CAPITAL LETTER YU
					*p = 0x044E;	//	ю	0xd1 0x8e	CYRILLIC SMALL LETTER YU
					break;
				case 0x042F:		//	Я	0xd0 0xaf	CYRILLIC CAPITAL LETTER YA
					*p = 0x044F;	//	я	0xd1 0x8f	CYRILLIC SMALL LETTER YA
					break;
				case 0x0460:		//	Ѡ	0xd1 0xa0	CYRILLIC CAPITAL LETTER OMEGA
					*p = 0x0461;	//	ѡ	0xd1 0xa1	CYRILLIC SMALL LETTER OMEGA
					break;
				case 0x0462:		//	Ѣ	0xd1 0xa2	CYRILLIC CAPITAL LETTER YAT
					*p = 0x0463;	//	ѣ	0xd1 0xa3	CYRILLIC SMALL LETTER YAT
					break;
				case 0x0464:		//	Ѥ	0xd1 0xa4	CYRILLIC CAPITAL LETTER IOTIFIED E
					*p = 0x0465;	//	ѥ	0xd1 0xa5	CYRILLIC SMALL LETTER IOTIFIED E
					break;
				case 0x0466:		//	Ѧ	0xd1 0xa6	CYRILLIC CAPITAL LETTER LITTLE YUS
					*p = 0x0467;	//	ѧ	0xd1 0xa7	CYRILLIC SMALL LETTER LITTLE YUS
					break;
				case 0x0468:		//	Ѩ	0xd1 0xa8	CYRILLIC CAPITAL LETTER IOTIFIED LITTLE YUS
					*p = 0x0469;	//	ѩ	0xd1 0xa9	CYRILLIC SMALL LETTER IOTIFIED LITTLE YUS
					break;
				case 0x046A:		//	Ѫ	0xd1 0xaa	CYRILLIC CAPITAL LETTER BIG YUS
					*p = 0x046B;	//	ѫ	0xd1 0xab	CYRILLIC SMALL LETTER BIG YUS
					break;
				case 0x046C:		//	Ѭ	0xd1 0xac	CYRILLIC CAPITAL LETTER IOTIFIED BIG YUS
					*p = 0x046D;	//	ѭ	0xd1 0xad	CYRILLIC SMALL LETTER IOTIFIED BIG YUS
					break;
				case 0x046E:		//	Ѯ	0xd1 0xae	CYRILLIC CAPITAL LETTER KSI
					*p = 0x046F;	//	ѯ	0xd1 0xaf	CYRILLIC SMALL LETTER KSI
					break;
				case 0x0470:		//	Ѱ	0xd1 0xb0	CYRILLIC CAPITAL LETTER PSI
					*p = 0x0471;	//	ѱ	0xd1 0xb1	CYRILLIC SMALL LETTER PSI
					break;
				case 0x0472:		//	Ѳ	0xd1 0xb2	CYRILLIC CAPITAL LETTER FITA
					*p = 0x0473;	//	ѳ	0xd1 0xb3	CYRILLIC SMALL LETTER FITA
					break;
				case 0x0474:		//	Ѵ	0xd1 0xb4	CYRILLIC CAPITAL LETTER IZHITSA
					*p = 0x0475;	//	ѵ	0xd1 0xb5	CYRILLIC SMALL LETTER IZHITSA
					break;
				case 0x0476:		//	Ѷ	0xd1 0xb6	CYRILLIC CAPITAL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT
					*p = 0x0477;	//	ѷ	0xd1 0xb7	CYRILLIC SMALL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT
					break;
				case 0x0478:		//	Ѹ	0xd1 0xb8	CYRILLIC CAPITAL LETTER UK
					*p = 0x0479;	//	ѹ	0xd1 0xb9	CYRILLIC SMALL LETTER UK
					break;
				case 0x047A:		//	Ѻ	0xd1 0xba	CYRILLIC CAPITAL LETTER ROUND OMEGA
					*p = 0x047B;	//	ѻ	0xd1 0xbb	CYRILLIC SMALL LETTER ROUND OMEGA
					break;
				case 0x047C:		//	Ѽ	0xd1 0xbc	CYRILLIC CAPITAL LETTER OMEGA WITH TITLO
					*p = 0x047D;	//	ѽ	0xd1 0xbd	CYRILLIC SMALL LETTER OMEGA WITH TITLO
					break;
				case 0x047E:		//	Ѿ	0xd1 0xbe	CYRILLIC CAPITAL LETTER OT
					*p = 0x047F;	//	ѿ	0xd1 0xbf	CYRILLIC SMALL LETTER OT
					break;
				case 0x0480:		//	Ҁ	0xd2 0x80	CYRILLIC CAPITAL LETTER KOPPA
					*p = 0x0481;	//	ҁ	0xd2 0x81	CYRILLIC SMALL LETTER KOPPA
					break;
				case 0x048A:		//	Ҋ	0xd2 0x8a	CYRILLIC CAPITAL LETTER SHORT I WITH TAIL
					*p = 0x048B;	//	ҋ	0xd2 0x8b	CYRILLIC SMALL LETTER SHORT I WITH TAIL
					break;
				case 0x048C:		//	Ҍ	0xd2 0x8c	CYRILLIC CAPITAL LETTER SEMISOFT SIGN
					*p = 0x048D;	//	ҍ	0xd2 0x8d	CYRILLIC SMALL LETTER SEMISOFT SIGN
					break;
				case 0x048E:		//	Ҏ	0xd2 0x8e	CYRILLIC CAPITAL LETTER ER WITH TICK
					*p = 0x048F;	//	ҏ	0xd2 0x8f	CYRILLIC SMALL LETTER ER WITH TICK
					break;
				case 0x0490:		//	Ґ	0xd2 0x90	CYRILLIC CAPITAL LETTER GHE WITH UPTURN
					*p = 0x0491;	//	ґ	0xd2 0x91	CYRILLIC SMALL LETTER GHE WITH UPTURN
					break;
				case 0x0492:		//	Ғ	0xd2 0x92	CYRILLIC CAPITAL LETTER GHE WITH STROKE
					*p = 0x0493;	//	ғ	0xd2 0x93	CYRILLIC SMALL LETTER GHE WITH STROKE
					break;
				case 0x0494:		//	Ҕ	0xd2 0x94	CYRILLIC CAPITAL LETTER GHE WITH MIDDLE HOOK
					*p = 0x0495;	//	ҕ	0xd2 0x95	CYRILLIC SMALL LETTER GHE WITH MIDDLE HOOK
					break;
				case 0x0496:		//	Җ	0xd2 0x96	CYRILLIC CAPITAL LETTER ZHE WITH DESCENDER
					*p = 0x0497;	//	җ	0xd2 0x97	CYRILLIC SMALL LETTER ZHE WITH DESCENDER
					break;
				case 0x0498:		//	Ҙ	0xd2 0x98	CYRILLIC CAPITAL LETTER ZE WITH DESCENDER
					*p = 0x0499;	//	ҙ	0xd2 0x99	CYRILLIC SMALL LETTER ZE WITH DESCENDER
					break;
				case 0x049A:		//	Қ	0xd2 0x9a	CYRILLIC CAPITAL LETTER KA WITH DESCENDER
					*p = 0x049B;	//	қ	0xd2 0x9b	CYRILLIC SMALL LETTER KA WITH DESCENDER
					break;
				case 0x049C:		//	Ҝ	0xd2 0x9c	CYRILLIC CAPITAL LETTER KA WITH VERTICAL STROKE
					*p = 0x049D;	//	ҝ	0xd2 0x9d	CYRILLIC SMALL LETTER KA WITH VERTICAL STROKE
					break;
				case 0x049E:		//	Ҟ	0xd2 0x9e	CYRILLIC CAPITAL LETTER KA WITH STROKE
					*p = 0x049F;	//	ҟ	0xd2 0x9f	CYRILLIC SMALL LETTER KA WITH STROKE
					break;
				case 0x04A0:		//	Ҡ	0xd2 0xa0	CYRILLIC CAPITAL LETTER BASHKIR KA
					*p = 0x04A1;	//	ҡ	0xd2 0xa1	CYRILLIC SMALL LETTER BASHKIR KA
					break;
				case 0x04A2:		//	Ң	0xd2 0xa2	CYRILLIC CAPITAL LETTER EN WITH DESCENDER
					*p = 0x04A3;	//	ң	0xd2 0xa3	CYRILLIC SMALL LETTER EN WITH DESCENDER
					break;
				case 0x04A4:		//	Ҥ	0xd2 0xa4	CYRILLIC CAPITAL LETTER LIGATURE EN GHE
					*p = 0x04A5;	//	ҥ	0xd2 0xa5	CYRILLIC SMALL LETTER LIGATURE EN GHE
					break;
				case 0x04A6:		//	Ҧ	0xd2 0xa6	CYRILLIC CAPITAL LETTER PE WITH MIDDLE HOOK
					*p = 0x04A7;	//	ҧ	0xd2 0xa7	CYRILLIC SMALL LETTER PE WITH MIDDLE HOOK
					break;
				case 0x04A8:		//	Ҩ	0xd2 0xa8	CYRILLIC CAPITAL LETTER ABKHASIAN HA
					*p = 0x04A9;	//	ҩ	0xd2 0xa9	CYRILLIC SMALL LETTER ABKHASIAN HA
					break;
				case 0x04AA:		//	Ҫ	0xd2 0xaa	CYRILLIC CAPITAL LETTER ES WITH DESCENDER
					*p = 0x04AB;	//	ҫ	0xd2 0xab	CYRILLIC SMALL LETTER ES WITH DESCENDER
					break;
				case 0x04AC:		//	Ҭ	0xd2 0xac	CYRILLIC CAPITAL LETTER TE WITH DESCENDER
					*p = 0x04AD;	//	ҭ	0xd2 0xad	CYRILLIC SMALL LETTER TE WITH DESCENDER
					break;
				case 0x04AE:		//	Ү	0xd2 0xae	CYRILLIC CAPITAL LETTER STRAIGHT U
					*p = 0x04AF;	//	ү	0xd2 0xaf	CYRILLIC SMALL LETTER STRAIGHT U
					break;
				case 0x04B0:		//	Ұ	0xd2 0xb0	CYRILLIC CAPITAL LETTER STRAIGHT U WITH STROKE
					*p = 0x04B1;	//	ұ	0xd2 0xb1	CYRILLIC SMALL LETTER STRAIGHT U WITH STROKE
					break;
				case 0x04B2:		//	Ҳ	0xd2 0xb2	CYRILLIC CAPITAL LETTER HA WITH DESCENDER
					*p = 0x04B3;	//	ҳ	0xd2 0xb3	CYRILLIC SMALL LETTER HA WITH DESCENDER
					break;
				case 0x04B4:		//	Ҵ	0xd2 0xb4	CYRILLIC CAPITAL LETTER LIGATURE TE TSE
					*p = 0x04B5;	//	ҵ	0xd2 0xb5	CYRILLIC SMALL LETTER LIGATURE TE TSE
					break;
				case 0x04B6:		//	Ҷ	0xd2 0xb6	CYRILLIC CAPITAL LETTER CHE WITH DESCENDER
					*p = 0x04B7;	//	ҷ	0xd2 0xb7	CYRILLIC SMALL LETTER CHE WITH DESCENDER
					break;
				case 0x04B8:		//	Ҹ	0xd2 0xb8	CYRILLIC CAPITAL LETTER CHE WITH VERTICAL STROKE
					*p = 0x04B9;	//	ҹ	0xd2 0xb9	CYRILLIC SMALL LETTER CHE WITH VERTICAL STROKE
					break;
				case 0x04BA:		//	Һ	0xd2 0xba	CYRILLIC CAPITAL LETTER SHHA
					*p = 0x04BB;	//	һ	0xd2 0xbb	CYRILLIC SMALL LETTER SHHA
					break;
				case 0x04BC:		//	Ҽ	0xd2 0xbc	CYRILLIC CAPITAL LETTER ABKHASIAN CHE
					*p = 0x04BD;	//	ҽ	0xd2 0xbd	CYRILLIC SMALL LETTER ABKHASIAN CHE
					break;
				case 0x04BE:		//	Ҿ	0xd2 0xbe	CYRILLIC CAPITAL LETTER ABKHASIAN CHE WITH DESCENDER
					*p = 0x04BF;	//	ҿ	0xd2 0xbf	CYRILLIC SMALL LETTER ABKHASIAN CHE WITH DESCENDER
					break;
				case 0x04C0:		//	Ӏ	0xd3 0x80	CYRILLIC CAPITAL LETTER PALOCHKA
					*p = 0x04CF;	//	ӏ	0xd3 0x8f	CYRILLIC SMALL LETTER PALOCHKA
					break;
				case 0x04C1:		//	Ӂ	0xd3 0x81	CYRILLIC CAPITAL LETTER ZHE WITH BREVE
					*p = 0x04C2;	//	ӂ	0xd3 0x82	CYRILLIC SMALL LETTER ZHE WITH BREVE
					break;
				case 0x04C3:		//	Ӄ	0xd3 0x83	CYRILLIC CAPITAL LETTER KA WITH HOOK
					*p = 0x04C4;	//	ӄ	0xd3 0x84	CYRILLIC SMALL LETTER KA WITH HOOK
					break;
				case 0x04C5:		//	Ӆ	0xd3 0x85	CYRILLIC CAPITAL LETTER EL WITH TAIL
					*p = 0x04C6;	//	ӆ	0xd3 0x86	CYRILLIC SMALL LETTER EL WITH TAIL
					break;
				case 0x04C7:		//	Ӈ	0xd3 0x87	CYRILLIC CAPITAL LETTER EN WITH HOOK
					*p = 0x04C8;	//	ӈ	0xd3 0x88	CYRILLIC SMALL LETTER EN WITH HOOK
					break;
				case 0x04C9:		//	Ӊ	0xd3 0x89	CYRILLIC CAPITAL LETTER EN WITH TAIL
					*p = 0x04CA;	//	ӊ	0xd3 0x8a	CYRILLIC SMALL LETTER EN WITH TAIL
					break;
				case 0x04CB:		//	Ӌ	0xd3 0x8b	CYRILLIC CAPITAL LETTER KHAKASSIAN CHE
					*p = 0x04CC;	//	ӌ	0xd3 0x8c	CYRILLIC SMALL LETTER KHAKASSIAN CHE
					break;
				case 0x04CD:		//	Ӎ	0xd3 0x8d	CYRILLIC CAPITAL LETTER EM WITH TAIL
					*p = 0x04CE;	//	ӎ	0xd3 0x8e	CYRILLIC SMALL LETTER EM WITH TAIL
					break;
				case 0x04D0:		//	Ӑ	0xd3 0x90	CYRILLIC CAPITAL LETTER A WITH BREVE
					*p = 0x04D1;	//	ӑ	0xd3 0x91	CYRILLIC SMALL LETTER A WITH BREVE
					break;
				case 0x04D2:		//	Ӓ	0xd3 0x92	CYRILLIC CAPITAL LETTER A WITH DIAERESIS
					*p = 0x04D3;	//	ӓ	0xd3 0x93	CYRILLIC SMALL LETTER A WITH DIAERESIS
					break;
				case 0x04D4:		//	Ӕ	0xd3 0x94	CYRILLIC CAPITAL LETTER LIGATURE A IE
					*p = 0x04D5;	//	ӕ	0xd3 0x95	CYRILLIC SMALL LETTER LIGATURE A IE
					break;
				case 0x04D6:		//	Ӗ	0xd3 0x96	CYRILLIC CAPITAL LETTER IE WITH BREVE
					*p = 0x04D7;	//	ӗ	0xd3 0x97	CYRILLIC SMALL LETTER IE WITH BREVE
					break;
				case 0x04D8:		//	Ә	0xd3 0x98	CYRILLIC CAPITAL LETTER SCHWA
					*p = 0x04D9;	//	ә	0xd3 0x99	CYRILLIC SMALL LETTER SCHWA
					break;
				case 0x04DA:		//	Ӛ	0xd3 0x9a	CYRILLIC CAPITAL LETTER SCHWA WITH DIAERESIS
					*p = 0x04DB;	//	ӛ	0xd3 0x9b	CYRILLIC SMALL LETTER SCHWA WITH DIAERESIS
					break;
				case 0x04DC:		//	Ӝ	0xd3 0x9c	CYRILLIC CAPITAL LETTER ZHE WITH DIAERESIS
					*p = 0x04DD;	//	ӝ	0xd3 0x9d	CYRILLIC SMALL LETTER ZHE WITH DIAERESIS
					break;
				case 0x04DE:		//	Ӟ	0xd3 0x9e	CYRILLIC CAPITAL LETTER ZE WITH DIAERESIS
					*p = 0x04DF;	//	ӟ	0xd3 0x9f	CYRILLIC SMALL LETTER ZE WITH DIAERESIS
					break;
				case 0x04E0:		//	Ӡ	0xd3 0xa0	CYRILLIC CAPITAL LETTER ABKHASIAN DZE
					*p = 0x04E1;	//	ӡ	0xd3 0xa1	CYRILLIC SMALL LETTER ABKHASIAN DZE
					break;
				case 0x04E2:		//	Ӣ	0xd3 0xa2	CYRILLIC CAPITAL LETTER I WITH MACRON
					*p = 0x04E3;	//	ӣ	0xd3 0xa3	CYRILLIC SMALL LETTER I WITH MACRON
					break;
				case 0x04E4:		//	Ӥ	0xd3 0xa4	CYRILLIC CAPITAL LETTER I WITH DIAERESIS
					*p = 0x04E5;	//	ӥ	0xd3 0xa5	CYRILLIC SMALL LETTER I WITH DIAERESIS
					break;
				case 0x04E6:		//	Ӧ	0xd3 0xa6	CYRILLIC CAPITAL LETTER O WITH DIAERESIS
					*p = 0x04E7;	//	ӧ	0xd3 0xa7	CYRILLIC SMALL LETTER O WITH DIAERESIS
					break;
				case 0x04E8:		//	Ө	0xd3 0xa8	CYRILLIC CAPITAL LETTER BARRED O
					*p = 0x04E9;	//	ө	0xd3 0xa9	CYRILLIC SMALL LETTER BARRED O
					break;
				case 0x04EA:		//	Ӫ	0xd3 0xaa	CYRILLIC CAPITAL LETTER BARRED O WITH DIAERESIS
					*p = 0x04EB;	//	ӫ	0xd3 0xab	CYRILLIC SMALL LETTER BARRED O WITH DIAERESIS
					break;
				case 0x04EC:		//	Ӭ	0xd3 0xac	CYRILLIC CAPITAL LETTER E WITH DIAERESIS
					*p = 0x04ED;	//	ӭ	0xd3 0xad	CYRILLIC SMALL LETTER E WITH DIAERESIS
					break;
				case 0x04EE:		//	Ӯ	0xd3 0xae	CYRILLIC CAPITAL LETTER U WITH MACRON
					*p = 0x04EF;	//	ӯ	0xd3 0xaf	CYRILLIC SMALL LETTER U WITH MACRON
					break;
				case 0x04F0:		//	Ӱ	0xd3 0xb0	CYRILLIC CAPITAL LETTER U WITH DIAERESIS
					*p = 0x04F1;	//	ӱ	0xd3 0xb1	CYRILLIC SMALL LETTER U WITH DIAERESIS
					break;
				case 0x04F2:		//	Ӳ	0xd3 0xb2	CYRILLIC CAPITAL LETTER U WITH DOUBLE ACUTE
					*p = 0x04F3;	//	ӳ	0xd3 0xb3	CYRILLIC SMALL LETTER U WITH DOUBLE ACUTE
					break;
				case 0x04F4:		//	Ӵ	0xd3 0xb4	CYRILLIC CAPITAL LETTER CHE WITH DIAERESIS
					*p = 0x04F5;	//	ӵ	0xd3 0xb5	CYRILLIC SMALL LETTER CHE WITH DIAERESIS
					break;
				case 0x04F6:		//	Ӷ	0xd3 0xb6	CYRILLIC CAPITAL LETTER GHE WITH DESCENDER
					*p = 0x04F7;	//	ӷ	0xd3 0xb7	CYRILLIC SMALL LETTER GHE WITH DESCENDER
					break;
				case 0x04F8:		//	Ӹ	0xd3 0xb8	CYRILLIC CAPITAL LETTER YERU WITH DIAERESIS
					*p = 0x04F9;	//	ӹ	0xd3 0xb9	CYRILLIC SMALL LETTER YERU WITH DIAERESIS
					break;
				case 0x04FA:		//	Ӻ	0xd3 0xba	CYRILLIC CAPITAL LETTER GHE WITH STROKE AND HOOK
					*p = 0x04FB;	//	ӻ	0xd3 0xbb	CYRILLIC SMALL LETTER GHE WITH STROKE AND HOOK
					break;
				case 0x04FC:		//	Ӽ	0xd3 0xbc	CYRILLIC CAPITAL LETTER HA WITH HOOK
					*p = 0x04FD;	//	ӽ	0xd3 0xbd	CYRILLIC SMALL LETTER HA WITH HOOK
					break;
				case 0x04FE:		//	Ӿ	0xd3 0xbe	CYRILLIC CAPITAL LETTER HA WITH STROKE
					*p = 0x04FF;	//	ӿ	0xd3 0xbf	CYRILLIC SMALL LETTER HA WITH STROKE
					break;
				case 0x0500:		//	Ԁ	0xd4 0x80	CYRILLIC CAPITAL LETTER KOMI DE
					*p = 0x0501;	//	ԁ	0xd4 0x81	CYRILLIC SMALL LETTER KOMI DE
					break;
				case 0x0502:		//	Ԃ	0xd4 0x82	CYRILLIC CAPITAL LETTER KOMI DJE
					*p = 0x0503;	//	ԃ	0xd4 0x83	CYRILLIC SMALL LETTER KOMI DJE
					break;
				case 0x0504:		//	Ԅ	0xd4 0x84	CYRILLIC CAPITAL LETTER KOMI ZJE
					*p = 0x0505;	//	ԅ	0xd4 0x85	CYRILLIC SMALL LETTER KOMI ZJE
					break;
				case 0x0506:		//	Ԇ	0xd4 0x86	CYRILLIC CAPITAL LETTER KOMI DZJE
					*p = 0x0507;	//	ԇ	0xd4 0x87	CYRILLIC SMALL LETTER KOMI DZJE
					break;
				case 0x0508:		//	Ԉ	0xd4 0x88	CYRILLIC CAPITAL LETTER KOMI LJE
					*p = 0x0509;	//	ԉ	0xd4 0x89	CYRILLIC SMALL LETTER KOMI LJE
					break;
				case 0x050A:		//	Ԋ	0xd4 0x8a	CYRILLIC CAPITAL LETTER KOMI NJE
					*p = 0x050B;	//	ԋ	0xd4 0x8b	CYRILLIC SMALL LETTER KOMI NJE
					break;
				case 0x050C:		//	Ԍ	0xd4 0x8c	CYRILLIC CAPITAL LETTER KOMI SJE
					*p = 0x050D;	//	ԍ	0xd4 0x8d	CYRILLIC SMALL LETTER KOMI SJE
					break;
				case 0x050E:		//	Ԏ	0xd4 0x8e	CYRILLIC CAPITAL LETTER KOMI TJE
					*p = 0x050F;	//	ԏ	0xd4 0x8f	CYRILLIC SMALL LETTER KOMI TJE
					break;
				case 0x0510:		//	Ԑ	0xd4 0x90	CYRILLIC CAPITAL LETTER REVERSED ZE
					*p = 0x0511;	//	ԑ	0xd4 0x91	CYRILLIC SMALL LETTER REVERSED ZE
					break;
				case 0x0512:		//	Ԓ	0xd4 0x92	CYRILLIC CAPITAL LETTER EL WITH HOOK
					*p = 0x0513;	//	ԓ	0xd4 0x93	CYRILLIC SMALL LETTER EL WITH HOOK
					break;
				case 0x0514:		//	Ԕ	0xd4 0x94	CYRILLIC CAPITAL LETTER LHA
					*p = 0x0515;	//	ԕ	0xd4 0x95	CYRILLIC SMALL LETTER LHA
					break;
				case 0x0516:		//	Ԗ	0xd4 0x96	CYRILLIC CAPITAL LETTER RHA
					*p = 0x0517;	//	ԗ	0xd4 0x97	CYRILLIC SMALL LETTER RHA
					break;
				case 0x0518:		//	Ԙ	0xd4 0x98	CYRILLIC CAPITAL LETTER YAE
					*p = 0x0519;	//	ԙ	0xd4 0x99	CYRILLIC SMALL LETTER YAE
					break;
				case 0x051A:		//	Ԛ	0xd4 0x9a	CYRILLIC CAPITAL LETTER QA
					*p = 0x051B;	//	ԛ	0xd4 0x9b	CYRILLIC SMALL LETTER QA
					break;
				case 0x051C:		//	Ԝ	0xd4 0x9c	CYRILLIC CAPITAL LETTER WE
					*p = 0x051D;	//	ԝ	0xd4 0x9d	CYRILLIC SMALL LETTER WE
					break;
				case 0x051E:		//	Ԟ	0xd4 0x9e	CYRILLIC CAPITAL LETTER ALEUT KA
					*p = 0x051F;	//	ԟ	0xd4 0x9f	CYRILLIC SMALL LETTER ALEUT KA
					break;
				case 0x0520:		//	Ԡ	0xd4 0xa0	CYRILLIC CAPITAL LETTER EL WITH MIDDLE HOOK
					*p = 0x0521;	//	ԡ	0xd4 0xa1	CYRILLIC SMALL LETTER EL WITH MIDDLE HOOK
					break;
				case 0x0522:		//	Ԣ	0xd4 0xa2	CYRILLIC CAPITAL LETTER EN WITH MIDDLE HOOK
					*p = 0x0523;	//	ԣ	0xd4 0xa3	CYRILLIC SMALL LETTER EN WITH MIDDLE HOOK
					break;
				case 0x0524:		//	Ԥ	0xd4 0xa4	CYRILLIC CAPITAL LETTER PE WITH DESCENDER
					*p = 0x0525;	//	ԥ	0xd4 0xa5	CYRILLIC SMALL LETTER PE WITH DESCENDER
					break;
				case 0x0526:		//	Ԧ	0xd4 0xa6	CYRILLIC CAPITAL LETTER SHHA WITH DESCENDER
					*p = 0x0527;	//	ԧ	0xd4 0xa7	CYRILLIC SMALL LETTER SHHA WITH DESCENDER
					break;
				case 0x0528:		//	Ԩ	0xd4 0xa8	CYRILLIC CAPITAL LETTER EN WITH LEFT HOOK
					*p = 0x0529;	//	ԩ	0xd4 0xa9	CYRILLIC SMALL LETTER EN WITH LEFT HOOK
					break;
				case 0x052A:		//	Ԫ	0xd4 0xaa	CYRILLIC CAPITAL LETTER DZZHE
					*p = 0x052B;	//	ԫ	0xd4 0xab	CYRILLIC SMALL LETTER DZZHE
					break;
				case 0x052C:		//	Ԭ	0xd4 0xac	CYRILLIC CAPITAL LETTER DCHE
					*p = 0x052D;	//	ԭ	0xd4 0xad	CYRILLIC SMALL LETTER DCHE
					break;
				case 0x052E:		//	Ԯ	0xd4 0xae	CYRILLIC CAPITAL LETTER EL WITH DESCENDER
					*p = 0x052F;	//	ԯ	0xd4 0xaf	CYRILLIC SMALL LETTER EL WITH DESCENDER
					break;
				case 0x0531:		//	Ա	0xd4 0xb1	ARMENIAN CAPITAL LETTER AYB
					*p = 0x0561;	//	ա	0xd5 0xa1	ARMENIAN SMALL LETTER AYB
					break;
				case 0x0532:		//	Բ	0xd4 0xb2	ARMENIAN CAPITAL LETTER BEN
					*p = 0x0562;	//	բ	0xd5 0xa2	ARMENIAN SMALL LETTER BEN
					break;
				case 0x0533:		//	Գ	0xd4 0xb3	ARMENIAN CAPITAL LETTER GIM
					*p = 0x0563;	//	գ	0xd5 0xa3	ARMENIAN SMALL LETTER GIM
					break;
				case 0x0534:		//	Դ	0xd4 0xb4	ARMENIAN CAPITAL LETTER DA
					*p = 0x0564;	//	դ	0xd5 0xa4	ARMENIAN SMALL LETTER DA
					break;
				case 0x0535:		//	Ե	0xd4 0xb5	ARMENIAN CAPITAL LETTER ECH
					*p = 0x0565;	//	ե	0xd5 0xa5	ARMENIAN SMALL LETTER ECH
					break;
				case 0x0536:		//	Զ	0xd4 0xb6	ARMENIAN CAPITAL LETTER ZA
					*p = 0x0566;	//	զ	0xd5 0xa6	ARMENIAN SMALL LETTER ZA
					break;
				case 0x0537:		//	Է	0xd4 0xb7	ARMENIAN CAPITAL LETTER EH
					*p = 0x0567;	//	է	0xd5 0xa7	ARMENIAN SMALL LETTER EH
					break;
				case 0x0538:		//	Ը	0xd4 0xb8	ARMENIAN CAPITAL LETTER ET
					*p = 0x0568;	//	ը	0xd5 0xa8	ARMENIAN SMALL LETTER ET
					break;
				case 0x0539:		//	Թ	0xd4 0xb9	ARMENIAN CAPITAL LETTER TO
					*p = 0x0569;	//	թ	0xd5 0xa9	ARMENIAN SMALL LETTER TO
					break;
				case 0x053A:		//	Ժ	0xd4 0xba	ARMENIAN CAPITAL LETTER ZHE
					*p = 0x056A;	//	ժ	0xd5 0xaa	ARMENIAN SMALL LETTER ZHE
					break;
				case 0x053B:		//	Ի	0xd4 0xbb	ARMENIAN CAPITAL LETTER INI
					*p = 0x056B;	//	ի	0xd5 0xab	ARMENIAN SMALL LETTER INI
					break;
				case 0x053C:		//	Լ	0xd4 0xbc	ARMENIAN CAPITAL LETTER LIWN
					*p = 0x056C;	//	լ	0xd5 0xac	ARMENIAN SMALL LETTER LIWN
					break;
				case 0x053D:		//	Խ	0xd4 0xbd	ARMENIAN CAPITAL LETTER XEH
					*p = 0x056D;	//	խ	0xd5 0xad	ARMENIAN SMALL LETTER XEH
					break;
				case 0x053E:		//	Ծ	0xd4 0xbe	ARMENIAN CAPITAL LETTER CA
					*p = 0x056E;	//	ծ	0xd5 0xae	ARMENIAN SMALL LETTER CA
					break;
				case 0x053F:		//	Կ	0xd4 0xbf	ARMENIAN CAPITAL LETTER KEN
					*p = 0x056F;	//	կ	0xd5 0xaf	ARMENIAN SMALL LETTER KEN
					break;
				case 0x0540:		//	Հ	0xd5 0x80	ARMENIAN CAPITAL LETTER HO
					*p = 0x0570;	//	հ	0xd5 0xb0	ARMENIAN SMALL LETTER HO
					break;
				case 0x0541:		//	Ձ	0xd5 0x81	ARMENIAN CAPITAL LETTER JA
					*p = 0x0571;	//	ձ	0xd5 0xb1	ARMENIAN SMALL LETTER JA
					break;
				case 0x0542:		//	Ղ	0xd5 0x82	ARMENIAN CAPITAL LETTER GHAD
					*p = 0x0572;	//	ղ	0xd5 0xb2	ARMENIAN SMALL LETTER GHAD
					break;
				case 0x0543:		//	Ճ	0xd5 0x83	ARMENIAN CAPITAL LETTER CHEH
					*p = 0x0573;	//	ճ	0xd5 0xb3	ARMENIAN SMALL LETTER CHEH
					break;
				case 0x0544:		//	Մ	0xd5 0x84	ARMENIAN CAPITAL LETTER MEN
					*p = 0x0574;	//	մ	0xd5 0xb4	ARMENIAN SMALL LETTER MEN
					break;
				case 0x0545:		//	Յ	0xd5 0x85	ARMENIAN CAPITAL LETTER YI
					*p = 0x0575;	//	յ	0xd5 0xb5	ARMENIAN SMALL LETTER YI
					break;
				case 0x0546:		//	Ն	0xd5 0x86	ARMENIAN CAPITAL LETTER NOW
					*p = 0x0576;	//	ն	0xd5 0xb6	ARMENIAN SMALL LETTER NOW
					break;
				case 0x0547:		//	Շ	0xd5 0x87	ARMENIAN CAPITAL LETTER SHA
					*p = 0x0577;	//	շ	0xd5 0xb7	ARMENIAN SMALL LETTER SHA
					break;
				case 0x0548:		//	Ո	0xd5 0x88	ARMENIAN CAPITAL LETTER VO
					*p = 0x0578;	//	ո	0xd5 0xb8	ARMENIAN SMALL LETTER VO
					break;
				case 0x0549:		//	Չ	0xd5 0x89	ARMENIAN CAPITAL LETTER CHA
					*p = 0x0579;	//	չ	0xd5 0xb9	ARMENIAN SMALL LETTER CHA
					break;
				case 0x054A:		//	Պ	0xd5 0x8a	ARMENIAN CAPITAL LETTER PEH
					*p = 0x057A;	//	պ	0xd5 0xba	ARMENIAN SMALL LETTER PEH
					break;
				case 0x054B:		//	Ջ	0xd5 0x8b	ARMENIAN CAPITAL LETTER JHEH
					*p = 0x057B;	//	ջ	0xd5 0xbb	ARMENIAN SMALL LETTER JHEH
					break;
				case 0x054C:		//	Ռ	0xd5 0x8c	ARMENIAN CAPITAL LETTER RA
					*p = 0x057C;	//	ռ	0xd5 0xbc	ARMENIAN SMALL LETTER RA
					break;
				case 0x054D:		//	Ս	0xd5 0x8d	ARMENIAN CAPITAL LETTER SEH
					*p = 0x057D;	//	ս	0xd5 0xbd	ARMENIAN SMALL LETTER SEH
					break;
				case 0x054E:		//	Վ	0xd5 0x8e	ARMENIAN CAPITAL LETTER VEW
					*p = 0x057E;	//	վ	0xd5 0xbe	ARMENIAN SMALL LETTER VEW
					break;
				case 0x054F:		//	Տ	0xd5 0x8f	ARMENIAN CAPITAL LETTER TIWN
					*p = 0x057F;	//	տ	0xd5 0xbf	ARMENIAN SMALL LETTER TIWN
					break;
				case 0x0550:		//	Ր	0xd5 0x90	ARMENIAN CAPITAL LETTER REH
					*p = 0x0580;	//	ր	0xd6 0x80	ARMENIAN SMALL LETTER REH
					break;
				case 0x0551:		//	Ց	0xd5 0x91	ARMENIAN CAPITAL LETTER CO
					*p = 0x0581;	//	ց	0xd6 0x81	ARMENIAN SMALL LETTER CO
					break;
				case 0x0552:		//	Ւ	0xd5 0x92	ARMENIAN CAPITAL LETTER YIWN
					*p = 0x0582;	//	ւ	0xd6 0x82	ARMENIAN SMALL LETTER YIWN
					break;
				case 0x0553:		//	Փ	0xd5 0x93	ARMENIAN CAPITAL LETTER PIWR
					*p = 0x0583;	//	փ	0xd6 0x83	ARMENIAN SMALL LETTER PIWR
					break;
				case 0x0554:		//	Ք	0xd5 0x94	ARMENIAN CAPITAL LETTER KEH
					*p = 0x0584;	//	ք	0xd6 0x84	ARMENIAN SMALL LETTER KEH
					break;
				case 0x0555:		//	Օ	0xd5 0x95	ARMENIAN CAPITAL LETTER OH
					*p = 0x0585;	//	օ	0xd6 0x85	ARMENIAN SMALL LETTER OH
					break;
				case 0x0556:		//	Ֆ	0xd5 0x96	ARMENIAN CAPITAL LETTER FEH
					*p = 0x0586;	//	ֆ	0xd6 0x86	ARMENIAN SMALL LETTER FEH
					break;
				case 0x10A0:		//	Ⴀ	0xe1 0x82 0xa0	GEORGIAN LETTER AN
					*p = 0x10D0;	//	ა	0xe1 0x83 0x90	GEORGIAN SMALL LETTER AN
					break;
				case 0x10A1:		//	Ⴁ	0xe1 0x82 0xa1	GEORGIAN LETTER BAN
					*p = 0x10D1;	//	ბ	0xe1 0x83 0x91	GEORGIAN SMALL LETTER BAN
					break;
				case 0x10A2:		//	Ⴂ	0xe1 0x82 0xa2	GEORGIAN LETTER GAN
					*p = 0x10D2;	//	გ	0xe1 0x83 0x92	GEORGIAN SMALL LETTER GAN
					break;
				case 0x10A3:		//	Ⴃ	0xe1 0x82 0xa3	GEORGIAN LETTER DON
					*p = 0x10D3;	//	დ	0xe1 0x83 0x93	GEORGIAN SMALL LETTER DON
					break;
				case 0x10A4:		//	Ⴄ	0xe1 0x82 0xa4	GEORGIAN LETTER EN
					*p = 0x10D4;	//	ე	0xe1 0x83 0x94	GEORGIAN SMALL LETTER EN
					break;
				case 0x10A5:		//	Ⴅ	0xe1 0x82 0xa5	GEORGIAN LETTER VIN
					*p = 0x10D5;	//	ვ	0xe1 0x83 0x95	GEORGIAN SMALL LETTER VIN
					break;
				case 0x10A6:		//	Ⴆ	0xe1 0x82 0xa6	GEORGIAN LETTER ZEN
					*p = 0x10D6;	//	ზ	0xe1 0x83 0x96	GEORGIAN SMALL LETTER ZEN
					break;
				case 0x10A7:		//	Ⴇ	0xe1 0x82 0xa7	GEORGIAN LETTER TAN
					*p = 0x10D7;	//	თ	0xe1 0x83 0x97	GEORGIAN SMALL LETTER TAN
					break;
				case 0x10A8:		//	Ⴈ	0xe1 0x82 0xa8	GEORGIAN LETTER IN
					*p = 0x10D8;	//	ი	0xe1 0x83 0x98	GEORGIAN SMALL LETTER IN
					break;
				case 0x10A9:		//	Ⴉ	0xe1 0x82 0xa9	GEORGIAN LETTER KAN
					*p = 0x10D9;	//	კ	0xe1 0x83 0x99	GEORGIAN SMALL LETTER KAN
					break;
				case 0x10AA:		//	Ⴊ	0xe1 0x82 0xaa	GEORGIAN LETTER LAS
					*p = 0x10DA;	//	ლ	0xe1 0x83 0x9a	GEORGIAN SMALL LETTER LAS
					break;
				case 0x10AB:		//	Ⴋ	0xe1 0x82 0xab	GEORGIAN LETTER MAN
					*p = 0x10DB;	//	მ	0xe1 0x83 0x9b	GEORGIAN SMALL LETTER MAN
					break;
				case 0x10AC:		//	Ⴌ	0xe1 0x82 0xac	GEORGIAN LETTER NAR
					*p = 0x10DC;	//	ნ	0xe1 0x83 0x9c	GEORGIAN SMALL LETTER NAR
					break;
				case 0x10AD:		//	Ⴍ	0xe1 0x82 0xad	GEORGIAN LETTER ON
					*p = 0x10DD;	//	ო	0xe1 0x83 0x9d	GEORGIAN SMALL LETTER ON
					break;
				case 0x10AE:		//	Ⴎ	0xe1 0x82 0xae	GEORGIAN LETTER PAR
					*p = 0x10DE;	//	პ	0xe1 0x83 0x9e	GEORGIAN SMALL LETTER PAR
					break;
				case 0x10AF:		//	Ⴏ	0xe1 0x82 0xaf	GEORGIAN LETTER ZHAR
					*p = 0x10DF;	//	ჟ	0xe1 0x83 0x9f	GEORGIAN SMALL LETTER ZHAR
					break;
				case 0x10B0:		//	Ⴐ	0xe1 0x82 0xb0	GEORGIAN LETTER RAE
					*p = 0x10E0;	//	რ	0xe1 0x83 0xa0	GEORGIAN SMALL LETTER RAE
					break;
				case 0x10B1:		//	Ⴑ	0xe1 0x82 0xb1	GEORGIAN LETTER SAN
					*p = 0x10E1;	//	ს	0xe1 0x83 0xa1	GEORGIAN SMALL LETTER SAN
					break;
				case 0x10B2:		//	Ⴒ	0xe1 0x82 0xb2	GEORGIAN LETTER TAR
					*p = 0x10E2;	//	ტ	0xe1 0x83 0xa2	GEORGIAN SMALL LETTER TAR
					break;
				case 0x10B3:		//	Ⴓ	0xe1 0x82 0xb3	GEORGIAN LETTER UN
					*p = 0x10E3;	//	უ	0xe1 0x83 0xa3	GEORGIAN SMALL LETTER UN
					break;
				case 0x10B4:		//	Ⴔ	0xe1 0x82 0xb4	GEORGIAN LETTER PHAR
					*p = 0x10E4;	//	ფ	0xe1 0x83 0xa4	GEORGIAN SMALL LETTER PHAR
					break;
				case 0x10B5:		//	Ⴕ	0xe1 0x82 0xb5	GEORGIAN LETTER KHAR
					*p = 0x10E5;	//	ქ	0xe1 0x83 0xa5	GEORGIAN SMALL LETTER KHAR
					break;
				case 0x10B6:		//	Ⴖ	0xe1 0x82 0xb6	GEORGIAN LETTER GHAN
					*p = 0x10E6;	//	ღ	0xe1 0x83 0xa6	GEORGIAN SMALL LETTER GHAN
					break;
				case 0x10B7:		//	Ⴗ	0xe1 0x82 0xb7	GEORGIAN LETTER QAR
					*p = 0x10E7;	//	ყ	0xe1 0x83 0xa7	GEORGIAN SMALL LETTER QAR
					break;
				case 0x10B8:		//	Ⴘ	0xe1 0x82 0xb8	GEORGIAN LETTER SHIN
					*p = 0x10E8;	//	შ	0xe1 0x83 0xa8	GEORGIAN SMALL LETTER SHIN
					break;
				case 0x10B9:		//	Ⴙ	0xe1 0x82 0xb9	GEORGIAN LETTER CHIN
					*p = 0x10E9;	//	ჩ	0xe1 0x83 0xa9	GEORGIAN SMALL LETTER CHIN
					break;
				case 0x10BA:		//	Ⴚ	0xe1 0x82 0xba	GEORGIAN LETTER CAN
					*p = 0x10EA;	//	ც	0xe1 0x83 0xaa	GEORGIAN SMALL LETTER CAN
					break;
				case 0x10BB:		//	Ⴛ	0xe1 0x82 0xbb	GEORGIAN LETTER JIL
					*p = 0x10EB;	//	ძ	0xe1 0x83 0xab	GEORGIAN SMALL LETTER JIL
					break;
				case 0x10BC:		//	Ⴜ	0xe1 0x82 0xbc	GEORGIAN LETTER CIL
					*p = 0x10EC;	//	წ	0xe1 0x83 0xac	GEORGIAN SMALL LETTER CIL
					break;
				case 0x10BD:		//	Ⴝ	0xe1 0x82 0xbd	GEORGIAN LETTER CHAR
					*p = 0x10ED;	//	ჭ	0xe1 0x83 0xad	GEORGIAN SMALL LETTER CHAR
					break;
				case 0x10BE:		//	Ⴞ	0xe1 0x82 0xbe	GEORGIAN LETTER XAN
					*p = 0x10EE;	//	ხ	0xe1 0x83 0xae	GEORGIAN SMALL LETTER XAN
					break;
				case 0x10BF:		//	Ⴟ	0xe1 0x82 0xbf	GEORGIAN LETTER JHAN
					*p = 0x10EF;	//	ჯ	0xe1 0x83 0xaf	GEORGIAN SMALL LETTER JHAN
					break;
				case 0x10C0:		//	Ⴠ	0xe1 0x83 0x80	GEORGIAN LETTER HAE
					*p = 0x10F0;	//	ჰ	0xe1 0x83 0xb0	GEORGIAN SMALL LETTER HAE
					break;
				case 0x10C1:		//	Ⴡ	0xe1 0x83 0x81	GEORGIAN LETTER HE
					*p = 0x10F1;	//	ჱ	0xe1 0x83 0xb1	GEORGIAN SMALL LETTER HE
					break;
				case 0x10C2:		//	Ⴢ	0xe1 0x83 0x82	GEORGIAN LETTER HIE
					*p = 0x10F2;	//	ჲ	0xe1 0x83 0xb2	GEORGIAN SMALL LETTER HIE
					break;
				case 0x10C3:		//	Ⴣ	0xe1 0x83 0x83	GEORGIAN LETTER WE
					*p = 0x10F3;	//	ჳ	0xe1 0x83 0xb3	GEORGIAN SMALL LETTER WE
					break;
				case 0x10C4:		//	Ⴤ	0xe1 0x83 0x84	GEORGIAN LETTER HAR
					*p = 0x10F4;	//	ჴ	0xe1 0x83 0xb4	GEORGIAN SMALL LETTER HAR
					break;
				case 0x10C5:		//	Ⴥ	0xe1 0x83 0x85	GEORGIAN LETTER HOE
					*p = 0x10F5;	//	ჵ	0xe1 0x83 0xb5	GEORGIAN SMALL LETTER HOE
					break;
				case 0x10C7:		//	Ⴧ	0xe1 0x83 0x87	GEORGIAN LETTER YN
					*p = 0x10F7;	//	ჷ	0xe1 0x83 0xb7	GEORGIAN SMALL LETTER YN
					break;
				case 0x10CD:		//	Ⴭ	0xe1 0x83 0x8d	GEORGIAN LETTER AEN
					*p = 0x10FD;	//	ჽ	0xe1 0x83 0xbd	GEORGIAN SMALL LETTER AEN
					break;
				case 0x13A0:		//	Ꭰ	0xe1 0x8e 0xa0	CHEROKEE CAPITAL LETTER A
					*p = 0xAB70;	//	ꭰ	0xea 0xad 0xb0	CHEROKEE SMALL LETTER A
					break;
				case 0x13A1:		//	Ꭱ	0xe1 0x8e 0xa1	CHEROKEE CAPITAL LETTER E
					*p = 0xAB71;	//	ꭱ	0xea 0xad 0xb1	CHEROKEE SMALL LETTER E
					break;
				case 0x13A2:		//	Ꭲ	0xe1 0x8e 0xa2	CHEROKEE CAPITAL LETTER I
					*p = 0xAB72;	//	ꭲ	0xea 0xad 0xb2	CHEROKEE SMALL LETTER I
					break;
				case 0x13A3:		//	Ꭳ	0xe1 0x8e 0xa3	CHEROKEE CAPITAL LETTER O
					*p = 0xAB73;	//	ꭳ	0xea 0xad 0xb3	CHEROKEE SMALL LETTER O
					break;
				case 0x13A4:		//	Ꭴ	0xe1 0x8e 0xa4	CHEROKEE CAPITAL LETTER U
					*p = 0xAB74;	//	ꭴ	0xea 0xad 0xb4	CHEROKEE SMALL LETTER U
					break;
				case 0x13A5:		//	Ꭵ	0xe1 0x8e 0xa5	CHEROKEE CAPITAL LETTER V
					*p = 0xAB75;	//	ꭵ	0xea 0xad 0xb5	CHEROKEE SMALL LETTER V
					break;
				case 0x13A6:		//	Ꭶ	0xe1 0x8e 0xa6	CHEROKEE CAPITAL LETTER GA
					*p = 0xAB76;	//	ꭶ	0xea 0xad 0xb6	CHEROKEE SMALL LETTER GA
					break;
				case 0x13A7:		//	Ꭷ	0xe1 0x8e 0xa7	CHEROKEE CAPITAL LETTER KA
					*p = 0xAB77;	//	ꭷ	0xea 0xad 0xb7	CHEROKEE SMALL LETTER KA
					break;
				case 0x13A8:		//	Ꭸ	0xe1 0x8e 0xa8	CHEROKEE CAPITAL LETTER GE
					*p = 0xAB78;	//	ꭸ	0xea 0xad 0xb8	CHEROKEE SMALL LETTER GE
					break;
				case 0x13A9:		//	Ꭹ	0xe1 0x8e 0xa9	CHEROKEE CAPITAL LETTER GI
					*p = 0xAB79;	//	ꭹ	0xea 0xad 0xb9	CHEROKEE SMALL LETTER GI
					break;
				case 0x13AA:		//	Ꭺ	0xe1 0x8e 0xaa	CHEROKEE CAPITAL LETTER GO
					*p = 0xAB7A;	//	ꭺ	0xea 0xad 0xba	CHEROKEE SMALL LETTER GO
					break;
				case 0x13AB:		//	Ꭻ	0xe1 0x8e 0xab	CHEROKEE CAPITAL LETTER GU
					*p = 0xAB7B;	//	ꭻ	0xea 0xad 0xbb	CHEROKEE SMALL LETTER GU
					break;
				case 0x13AC:		//	Ꭼ	0xe1 0x8e 0xac	CHEROKEE CAPITAL LETTER GV
					*p = 0xAB7C;	//	ꭼ	0xea 0xad 0xbc	CHEROKEE SMALL LETTER GV
					break;
				case 0x13AD:		//	Ꭽ	0xe1 0x8e 0xad	CHEROKEE CAPITAL LETTER HA
					*p = 0xAB7D;	//	ꭽ	0xea 0xad 0xbd	CHEROKEE SMALL LETTER HA
					break;
				case 0x13AE:		//	Ꭾ	0xe1 0x8e 0xae	CHEROKEE CAPITAL LETTER HE
					*p = 0xAB7E;	//	ꭾ	0xea 0xad 0xbe	CHEROKEE SMALL LETTER HE
					break;
				case 0x13AF:		//	Ꭿ	0xe1 0x8e 0xaf	CHEROKEE CAPITAL LETTER HI
					*p = 0xAB7F;	//	ꭿ	0xea 0xad 0xbf	CHEROKEE SMALL LETTER HI
					break;
				case 0x13B0:		//	Ꮀ	0xe1 0x8e 0xb0	CHEROKEE CAPITAL LETTER HO
					*p = 0xAB80;	//	ꮀ	0xea 0xae 0x80	CHEROKEE SMALL LETTER HO
					break;
				case 0x13B1:		//	Ꮁ	0xe1 0x8e 0xb1	CHEROKEE CAPITAL LETTER HU
					*p = 0xAB81;	//	ꮁ	0xea 0xae 0x81	CHEROKEE SMALL LETTER HU
					break;
				case 0x13B2:		//	Ꮂ	0xe1 0x8e 0xb2	CHEROKEE CAPITAL LETTER HV
					*p = 0xAB82;	//	ꮂ	0xea 0xae 0x82	CHEROKEE SMALL LETTER HV
					break;
				case 0x13B3:		//	Ꮃ	0xe1 0x8e 0xb3	CHEROKEE CAPITAL LETTER LA
					*p = 0xAB83;	//	ꮃ	0xea 0xae 0x83	CHEROKEE SMALL LETTER LA
					break;
				case 0x13B4:		//	Ꮄ	0xe1 0x8e 0xb4	CHEROKEE CAPITAL LETTER LE
					*p = 0xAB84;	//	ꮄ	0xea 0xae 0x84	CHEROKEE SMALL LETTER LE
					break;
				case 0x13B5:		//	Ꮅ	0xe1 0x8e 0xb5	CHEROKEE CAPITAL LETTER LI
					*p = 0xAB85;	//	ꮅ	0xea 0xae 0x85	CHEROKEE SMALL LETTER LI
					break;
				case 0x13B6:		//	Ꮆ	0xe1 0x8e 0xb6	CHEROKEE CAPITAL LETTER LO
					*p = 0xAB86;	//	ꮆ	0xea 0xae 0x86	CHEROKEE SMALL LETTER LO
					break;
				case 0x13B7:		//	Ꮇ	0xe1 0x8e 0xb7	CHEROKEE CAPITAL LETTER LU
					*p = 0xAB87;	//	ꮇ	0xea 0xae 0x87	CHEROKEE SMALL LETTER LU
					break;
				case 0x13B8:		//	Ꮈ	0xe1 0x8e 0xb8	CHEROKEE CAPITAL LETTER LV
					*p = 0xAB88;	//	ꮈ	0xea 0xae 0x88	CHEROKEE SMALL LETTER LV
					break;
				case 0x13B9:		//	Ꮉ	0xe1 0x8e 0xb9	CHEROKEE CAPITAL LETTER MA
					*p = 0xAB89;	//	ꮉ	0xea 0xae 0x89	CHEROKEE SMALL LETTER MA
					break;
				case 0x13BA:		//	Ꮊ	0xe1 0x8e 0xba	CHEROKEE CAPITAL LETTER ME
					*p = 0xAB8A;	//	ꮊ	0xea 0xae 0x8a	CHEROKEE SMALL LETTER ME
					break;
				case 0x13BB:		//	Ꮋ	0xe1 0x8e 0xbb	CHEROKEE CAPITAL LETTER MI
					*p = 0xAB8B;	//	ꮋ	0xea 0xae 0x8b	CHEROKEE SMALL LETTER MI
					break;
				case 0x13BC:		//	Ꮌ	0xe1 0x8e 0xbc	CHEROKEE CAPITAL LETTER MO
					*p = 0xAB8C;	//	ꮌ	0xea 0xae 0x8c	CHEROKEE SMALL LETTER MO
					break;
				case 0x13BD:		//	Ꮍ	0xe1 0x8e 0xbd	CHEROKEE CAPITAL LETTER MU
					*p = 0xAB8D;	//	ꮍ	0xea 0xae 0x8d	CHEROKEE SMALL LETTER MU
					break;
				case 0x13BE:		//	Ꮎ	0xe1 0x8e 0xbe	CHEROKEE CAPITAL LETTER NA
					*p = 0xAB8E;	//	ꮎ	0xea 0xae 0x8e	CHEROKEE SMALL LETTER NA
					break;
				case 0x13BF:		//	Ꮏ	0xe1 0x8e 0xbf	CHEROKEE CAPITAL LETTER HNA
					*p = 0xAB8F;	//	ꮏ	0xea 0xae 0x8f	CHEROKEE SMALL LETTER HNA
					break;
				case 0x13C0:		//	Ꮐ	0xe1 0x8f 0x80	CHEROKEE CAPITAL LETTER NAH
					*p = 0xAB90;	//	ꮐ	0xea 0xae 0x90	CHEROKEE SMALL LETTER NAH
					break;
				case 0x13C1:		//	Ꮑ	0xe1 0x8f 0x81	CHEROKEE CAPITAL LETTER NE
					*p = 0xAB91;	//	ꮑ	0xea 0xae 0x91	CHEROKEE SMALL LETTER NE
					break;
				case 0x13C2:		//	Ꮒ	0xe1 0x8f 0x82	CHEROKEE CAPITAL LETTER NI
					*p = 0xAB92;	//	ꮒ	0xea 0xae 0x92	CHEROKEE SMALL LETTER NI
					break;
				case 0x13C3:		//	Ꮓ	0xe1 0x8f 0x83	CHEROKEE CAPITAL LETTER NO
					*p = 0xAB93;	//	ꮓ	0xea 0xae 0x93	CHEROKEE SMALL LETTER NO
					break;
				case 0x13C4:		//	Ꮔ	0xe1 0x8f 0x84	CHEROKEE CAPITAL LETTER NU
					*p = 0xAB94;	//	ꮔ	0xea 0xae 0x94	CHEROKEE SMALL LETTER NU
					break;
				case 0x13C5:		//	Ꮕ	0xe1 0x8f 0x85	CHEROKEE CAPITAL LETTER NV
					*p = 0xAB95;	//	ꮕ	0xea 0xae 0x95	CHEROKEE SMALL LETTER NV
					break;
				case 0x13C6:		//	Ꮖ	0xe1 0x8f 0x86	CHEROKEE CAPITAL LETTER QUA
					*p = 0xAB96;	//	ꮖ	0xea 0xae 0x96	CHEROKEE SMALL LETTER QUA
					break;
				case 0x13C7:		//	Ꮗ	0xe1 0x8f 0x87	CHEROKEE CAPITAL LETTER QUE
					*p = 0xAB97;	//	ꮗ	0xea 0xae 0x97	CHEROKEE SMALL LETTER QUE
					break;
				case 0x13C8:		//	Ꮘ	0xe1 0x8f 0x88	CHEROKEE CAPITAL LETTER QUI
					*p = 0xAB98;	//	ꮘ	0xea 0xae 0x98	CHEROKEE SMALL LETTER QUI
					break;
				case 0x13C9:		//	Ꮙ	0xe1 0x8f 0x89	CHEROKEE CAPITAL LETTER QUO
					*p = 0xAB99;	//	ꮙ	0xea 0xae 0x99	CHEROKEE SMALL LETTER QUO
					break;
				case 0x13CA:		//	Ꮚ	0xe1 0x8f 0x8a	CHEROKEE CAPITAL LETTER QUU
					*p = 0xAB9A;	//	ꮚ	0xea 0xae 0x9a	CHEROKEE SMALL LETTER QUU
					break;
				case 0x13CB:		//	Ꮛ	0xe1 0x8f 0x8b	CHEROKEE CAPITAL LETTER QUV
					*p = 0xAB9B;	//	ꮛ	0xea 0xae 0x9b	CHEROKEE SMALL LETTER QUV
					break;
				case 0x13CC:		//	Ꮜ	0xe1 0x8f 0x8c	CHEROKEE CAPITAL LETTER SA
					*p = 0xAB9C;	//	ꮜ	0xea 0xae 0x9c	CHEROKEE SMALL LETTER SA
					break;
				case 0x13CD:		//	Ꮝ	0xe1 0x8f 0x8d	CHEROKEE CAPITAL LETTER S
					*p = 0xAB9D;	//	ꮝ	0xea 0xae 0x9d	CHEROKEE SMALL LETTER S
					break;
				case 0x13CE:		//	Ꮞ	0xe1 0x8f 0x8e	CHEROKEE CAPITAL LETTER SE
					*p = 0xAB9E;	//	ꮞ	0xea 0xae 0x9e	CHEROKEE SMALL LETTER SE
					break;
				case 0x13CF:		//	Ꮟ	0xe1 0x8f 0x8f	CHEROKEE CAPITAL LETTER SI
					*p = 0xAB9F;	//	ꮟ	0xea 0xae 0x9f	CHEROKEE SMALL LETTER SI
					break;
				case 0x13D0:		//	Ꮠ	0xe1 0x8f 0x90	CHEROKEE CAPITAL LETTER SO
					*p = 0xABA0;	//	ꮠ	0xea 0xae 0xa0	CHEROKEE SMALL LETTER SO
					break;
				case 0x13D1:		//	Ꮡ	0xe1 0x8f 0x91	CHEROKEE CAPITAL LETTER SU
					*p = 0xABA1;	//	ꮡ	0xea 0xae 0xa1	CHEROKEE SMALL LETTER SU
					break;
				case 0x13D2:		//	Ꮢ	0xe1 0x8f 0x92	CHEROKEE CAPITAL LETTER SV
					*p = 0xABA2;	//	ꮢ	0xea 0xae 0xa2	CHEROKEE SMALL LETTER SV
					break;
				case 0x13D3:		//	Ꮣ	0xe1 0x8f 0x93	CHEROKEE CAPITAL LETTER DA
					*p = 0xABA3;	//	ꮣ	0xea 0xae 0xa3	CHEROKEE SMALL LETTER DA
					break;
				case 0x13D4:		//	Ꮤ	0xe1 0x8f 0x94	CHEROKEE CAPITAL LETTER TA
					*p = 0xABA4;	//	ꮤ	0xea 0xae 0xa4	CHEROKEE SMALL LETTER TA
					break;
				case 0x13D5:		//	Ꮥ	0xe1 0x8f 0x95	CHEROKEE CAPITAL LETTER DE
					*p = 0xABA5;	//	ꮥ	0xea 0xae 0xa5	CHEROKEE SMALL LETTER DE
					break;
				case 0x13D6:		//	Ꮦ	0xe1 0x8f 0x96	CHEROKEE CAPITAL LETTER TE
					*p = 0xABA6;	//	ꮦ	0xea 0xae 0xa6	CHEROKEE SMALL LETTER TE
					break;
				case 0x13D7:		//	Ꮧ	0xe1 0x8f 0x97	CHEROKEE CAPITAL LETTER DI
					*p = 0xABA7;	//	ꮧ	0xea 0xae 0xa7	CHEROKEE SMALL LETTER DI
					break;
				case 0x13D8:		//	Ꮨ	0xe1 0x8f 0x98	CHEROKEE CAPITAL LETTER TI
					*p = 0xABA8;	//	ꮨ	0xea 0xae 0xa8	CHEROKEE SMALL LETTER TI
					break;
				case 0x13D9:		//	Ꮩ	0xe1 0x8f 0x99	CHEROKEE CAPITAL LETTER DO
					*p = 0xABA9;	//	ꮩ	0xea 0xae 0xa9	CHEROKEE SMALL LETTER DO
					break;
				case 0x13DA:		//	Ꮪ	0xe1 0x8f 0x9a	CHEROKEE CAPITAL LETTER DU
					*p = 0xABAA;	//	ꮪ	0xea 0xae 0xaa	CHEROKEE SMALL LETTER DU
					break;
				case 0x13DB:		//	Ꮫ	0xe1 0x8f 0x9b	CHEROKEE CAPITAL LETTER DV
					*p = 0xABAB;	//	ꮫ	0xea 0xae 0xab	CHEROKEE SMALL LETTER DV
					break;
				case 0x13DC:		//	Ꮬ	0xe1 0x8f 0x9c	CHEROKEE CAPITAL LETTER DLA
					*p = 0xABAC;	//	ꮬ	0xea 0xae 0xac	CHEROKEE SMALL LETTER DLA
					break;
				case 0x13DD:		//	Ꮭ	0xe1 0x8f 0x9d	CHEROKEE CAPITAL LETTER TLA
					*p = 0xABAD;	//	ꮭ	0xea 0xae 0xad	CHEROKEE SMALL LETTER TLA
					break;
				case 0x13DE:		//	Ꮮ	0xe1 0x8f 0x9e	CHEROKEE CAPITAL LETTER TLE
					*p = 0xABAE;	//	ꮮ	0xea 0xae 0xae	CHEROKEE SMALL LETTER TLE
					break;
				case 0x13DF:		//	Ꮯ	0xe1 0x8f 0x9f	CHEROKEE CAPITAL LETTER TLI
					*p = 0xABAF;	//	ꮯ	0xea 0xae 0xaf	CHEROKEE SMALL LETTER TLI
					break;
				case 0x13E0:		//	Ꮰ	0xe1 0x8f 0xa0	CHEROKEE CAPITAL LETTER TLO
					*p = 0xABB0;	//	ꮰ	0xea 0xae 0xb0	CHEROKEE SMALL LETTER TLO
					break;
				case 0x13E1:		//	Ꮱ	0xe1 0x8f 0xa1	CHEROKEE CAPITAL LETTER TLU
					*p = 0xABB1;	//	ꮱ	0xea 0xae 0xb1	CHEROKEE SMALL LETTER TLU
					break;
				case 0x13E2:		//	Ꮲ	0xe1 0x8f 0xa2	CHEROKEE CAPITAL LETTER TLV
					*p = 0xABB2;	//	ꮲ	0xea 0xae 0xb2	CHEROKEE SMALL LETTER TLV
					break;
				case 0x13E3:		//	Ꮳ	0xe1 0x8f 0xa3	CHEROKEE CAPITAL LETTER TSA
					*p = 0xABB3;	//	ꮳ	0xea 0xae 0xb3	CHEROKEE SMALL LETTER TSA
					break;
				case 0x13E4:		//	Ꮴ	0xe1 0x8f 0xa4	CHEROKEE CAPITAL LETTER TSE
					*p = 0xABB4;	//	ꮴ	0xea 0xae 0xb4	CHEROKEE SMALL LETTER TSE
					break;
				case 0x13E5:		//	Ꮵ	0xe1 0x8f 0xa5	CHEROKEE CAPITAL LETTER TSI
					*p = 0xABB5;	//	ꮵ	0xea 0xae 0xb5	CHEROKEE SMALL LETTER TSI
					break;
				case 0x13E6:		//	Ꮶ	0xe1 0x8f 0xa6	CHEROKEE CAPITAL LETTER TSO
					*p = 0xABB6;	//	ꮶ	0xea 0xae 0xb6	CHEROKEE SMALL LETTER TSO
					break;
				case 0x13E7:		//	Ꮷ	0xe1 0x8f 0xa7	CHEROKEE CAPITAL LETTER TSU
					*p = 0xABB7;	//	ꮷ	0xea 0xae 0xb7	CHEROKEE SMALL LETTER TSU
					break;
				case 0x13E8:		//	Ꮸ	0xe1 0x8f 0xa8	CHEROKEE CAPITAL LETTER TSV
					*p = 0xABB8;	//	ꮸ	0xea 0xae 0xb8	CHEROKEE SMALL LETTER TSV
					break;
				case 0x13E9:		//	Ꮹ	0xe1 0x8f 0xa9	CHEROKEE CAPITAL LETTER WA
					*p = 0xABB9;	//	ꮹ	0xea 0xae 0xb9	CHEROKEE SMALL LETTER WA
					break;
				case 0x13EA:		//	Ꮺ	0xe1 0x8f 0xaa	CHEROKEE CAPITAL LETTER WE
					*p = 0xABBA;	//	ꮺ	0xea 0xae 0xba	CHEROKEE SMALL LETTER WE
					break;
				case 0x13EB:		//	Ꮻ	0xe1 0x8f 0xab	CHEROKEE CAPITAL LETTER WI
					*p = 0xABBB;	//	ꮻ	0xea 0xae 0xbb	CHEROKEE SMALL LETTER WI
					break;
				case 0x13EC:		//	Ꮼ	0xe1 0x8f 0xac	CHEROKEE CAPITAL LETTER WO
					*p = 0xABBC;	//	ꮼ	0xea 0xae 0xbc	CHEROKEE SMALL LETTER WO
					break;
				case 0x13ED:		//	Ꮽ	0xe1 0x8f 0xad	CHEROKEE CAPITAL LETTER WU
					*p = 0xABBD;	//	ꮽ	0xea 0xae 0xbd	CHEROKEE SMALL LETTER WU
					break;
				case 0x13EE:		//	Ꮾ	0xe1 0x8f 0xae	CHEROKEE CAPITAL LETTER WV
					*p = 0xABBE;	//	ꮾ	0xea 0xae 0xbe	CHEROKEE SMALL LETTER WV
					break;
				case 0x13EF:		//	Ꮿ	0xe1 0x8f 0xaf	CHEROKEE CAPITAL LETTER YA
					*p = 0xABBF;	//	ꮿ	0xea 0xae 0xbf	CHEROKEE SMALL LETTER YA
					break;
				case 0x13F0:		//	Ᏸ	0xe1 0x8f 0xb0	CHEROKEE CAPITAL LETTER YE
					*p = 0x13F8;	//	ᏸ	0xe1 0x8f 0xb8	CHEROKEE SMALL LETTER YE
					break;
				case 0x13F1:		//	Ᏹ	0xe1 0x8f 0xb1	CHEROKEE CAPITAL LETTER YI
					*p = 0x13F9;	//	ᏹ	0xe1 0x8f 0xb9	CHEROKEE SMALL LETTER YI
					break;
				case 0x13F2:		//	Ᏺ	0xe1 0x8f 0xb2	CHEROKEE CAPITAL LETTER YO
					*p = 0x13FA;	//	ᏺ	0xe1 0x8f 0xba	CHEROKEE SMALL LETTER YO
					break;
				case 0x13F3:		//	Ᏻ	0xe1 0x8f 0xb3	CHEROKEE CAPITAL LETTER YU
					*p = 0x13FB;	//	ᏻ	0xe1 0x8f 0xbb	CHEROKEE SMALL LETTER YU
					break;
				case 0x13F4:		//	Ᏼ	0xe1 0x8f 0xb4	CHEROKEE CAPITAL LETTER YV
					*p = 0x13FC;	//	ᏼ	0xe1 0x8f 0xbc	CHEROKEE SMALL LETTER YV
					break;
				case 0x13F5:		//	Ᏽ	0xe1 0x8f 0xb5	CHEROKEE CAPITAL LETTER MV
					*p = 0x13FD;	//	ᏽ	0xe1 0x8f 0xbd	CHEROKEE SMALL LETTER MV
					break;
				case 0x1C90:		//	Ა	0xe1 0xb2 0x90	GEORGIAN CAPITAL LETTER AN
					*p = 0x10D0;	//	ა	0xe1 0x83 0x90	GEORGIAN SMALL LETTER AN
					break;
				case 0x1C91:		//	Ბ	0xe1 0xb2 0x91	GEORGIAN CAPITAL LETTER BAN
					*p = 0x10D1;	//	ბ	0xe1 0x83 0x91	GEORGIAN SMALL LETTER BAN
					break;
				case 0x1C92:		//	Გ	0xe1 0xb2 0x92	GEORGIAN CAPITAL LETTER GAN
					*p = 0x10D2;	//	გ	0xe1 0x83 0x92	GEORGIAN SMALL LETTER GAN
					break;
				case 0x1C93:		//	Დ	0xe1 0xb2 0x93	GEORGIAN CAPITAL LETTER DON
					*p = 0x10D3;	//	დ	0xe1 0x83 0x93	GEORGIAN SMALL LETTER DON
					break;
				case 0x1C94:		//	Ე	0xe1 0xb2 0x94	GEORGIAN CAPITAL LETTER EN
					*p = 0x10D4;	//	ე	0xe1 0x83 0x94	GEORGIAN SMALL LETTER EN
					break;
				case 0x1C95:		//	Ვ	0xe1 0xb2 0x95	GEORGIAN CAPITAL LETTER VIN
					*p = 0x10D5;	//	ვ	0xe1 0x83 0x95	GEORGIAN SMALL LETTER VIN
					break;
				case 0x1C96:		//	Ზ	0xe1 0xb2 0x96	GEORGIAN CAPITAL LETTER ZEN
					*p = 0x10D6;	//	ზ	0xe1 0x83 0x96	GEORGIAN SMALL LETTER ZEN
					break;
				case 0x1C97:		//	Თ	0xe1 0xb2 0x97	GEORGIAN CAPITAL LETTER TAN
					*p = 0x10D7;	//	თ	0xe1 0x83 0x97	GEORGIAN SMALL LETTER TAN
					break;
				case 0x1C98:		//	Ი	0xe1 0xb2 0x98	GEORGIAN CAPITAL LETTER IN
					*p = 0x10D8;	//	ი	0xe1 0x83 0x98	GEORGIAN SMALL LETTER IN
					break;
				case 0x1C99:		//	Კ	0xe1 0xb2 0x99	GEORGIAN CAPITAL LETTER KAN
					*p = 0x10D9;	//	კ	0xe1 0x83 0x99	GEORGIAN SMALL LETTER KAN
					break;
				case 0x1C9A:		//	Ლ	0xe1 0xb2 0x9a	GEORGIAN CAPITAL LETTER LAS
					*p = 0x10DA;	//	ლ	0xe1 0x83 0x9a	GEORGIAN SMALL LETTER LAS
					break;
				case 0x1C9B:		//	Მ	0xe1 0xb2 0x9b	GEORGIAN CAPITAL LETTER MAN
					*p = 0x10DB;	//	მ	0xe1 0x83 0x9b	GEORGIAN SMALL LETTER MAN
					break;
				case 0x1C9C:		//	Ნ	0xe1 0xb2 0x9c	GEORGIAN CAPITAL LETTER NAR
					*p = 0x10DC;	//	ნ	0xe1 0x83 0x9c	GEORGIAN SMALL LETTER NAR
					break;
				case 0x1C9D:		//	Ო	0xe1 0xb2 0x9d	GEORGIAN CAPITAL LETTER ON
					*p = 0x10DD;	//	ო	0xe1 0x83 0x9d	GEORGIAN SMALL LETTER ON
					break;
				case 0x1C9E:		//	Პ	0xe1 0xb2 0x9e	GEORGIAN CAPITAL LETTER PAR
					*p = 0x10DE;	//	პ	0xe1 0x83 0x9e	GEORGIAN SMALL LETTER PAR
					break;
				case 0x1C9F:		//	Ჟ	0xe1 0xb2 0x9f	GEORGIAN CAPITAL LETTER ZHAR
					*p = 0x10DF;	//	ჟ	0xe1 0x83 0x9f	GEORGIAN SMALL LETTER ZHAR
					break;
				case 0x1CA0:		//	Რ	0xe1 0xb2 0xa0	GEORGIAN CAPITAL LETTER RAE
					*p = 0x10E0;	//	რ	0xe1 0x83 0xa0	GEORGIAN SMALL LETTER RAE
					break;
				case 0x1CA1:		//	Ს	0xe1 0xb2 0xa1	GEORGIAN CAPITAL LETTER SAN
					*p = 0x10E1;	//	ს	0xe1 0x83 0xa1	GEORGIAN SMALL LETTER SAN
					break;
				case 0x1CA2:		//	Ტ	0xe1 0xb2 0xa2	GEORGIAN CAPITAL LETTER TAR
					*p = 0x10E2;	//	ტ	0xe1 0x83 0xa2	GEORGIAN SMALL LETTER TAR
					break;
				case 0x1CA3:		//	Უ	0xe1 0xb2 0xa3	GEORGIAN CAPITAL LETTER UN
					*p = 0x10E3;	//	უ	0xe1 0x83 0xa3	GEORGIAN SMALL LETTER UN
					break;
				case 0x1CA4:		//	Ფ	0xe1 0xb2 0xa4	GEORGIAN CAPITAL LETTER PHAR
					*p = 0x10E4;	//	ფ	0xe1 0x83 0xa4	GEORGIAN SMALL LETTER PHAR
					break;
				case 0x1CA5:		//	Ქ	0xe1 0xb2 0xa5	GEORGIAN CAPITAL LETTER KHAR
					*p = 0x10E5;	//	ქ	0xe1 0x83 0xa5	GEORGIAN SMALL LETTER KHAR
					break;
				case 0x1CA6:		//	Ღ	0xe1 0xb2 0xa6	GEORGIAN CAPITAL LETTER GHAN
					*p = 0x10E6;	//	ღ	0xe1 0x83 0xa6	GEORGIAN SMALL LETTER GHAN
					break;
				case 0x1CA7:		//	Ყ	0xe1 0xb2 0xa7	GEORGIAN CAPITAL LETTER QAR
					*p = 0x10E7;	//	ყ	0xe1 0x83 0xa7	GEORGIAN SMALL LETTER QAR
					break;
				case 0x1CA8:		//	Შ	0xe1 0xb2 0xa8	GEORGIAN CAPITAL LETTER SHIN
					*p = 0x10E8;	//	შ	0xe1 0x83 0xa8	GEORGIAN SMALL LETTER SHIN
					break;
				case 0x1CA9:		//	Ჩ	0xe1 0xb2 0xa9	GEORGIAN CAPITAL LETTER CHIN
					*p = 0x10E9;	//	ჩ	0xe1 0x83 0xa9	GEORGIAN SMALL LETTER CHIN
					break;
				case 0x1CAA:		//	Ც	0xe1 0xb2 0xaa	GEORGIAN CAPITAL LETTER CAN
					*p = 0x10EA;	//	ც	0xe1 0x83 0xaa	GEORGIAN SMALL LETTER CAN
					break;
				case 0x1CAB:		//	Ძ	0xe1 0xb2 0xab	GEORGIAN CAPITAL LETTER JIL
					*p = 0x10EB;	//	ძ	0xe1 0x83 0xab	GEORGIAN SMALL LETTER JIL
					break;
				case 0x1CAC:		//	Წ	0xe1 0xb2 0xac	GEORGIAN CAPITAL LETTER CIL
					*p = 0x10EC;	//	წ	0xe1 0x83 0xac	GEORGIAN SMALL LETTER CIL
					break;
				case 0x1CAD:		//	Ჭ	0xe1 0xb2 0xad	GEORGIAN CAPITAL LETTER CHAR
					*p = 0x10ED;	//	ჭ	0xe1 0x83 0xad	GEORGIAN SMALL LETTER CHAR
					break;
				case 0x1CAE:		//	Ხ	0xe1 0xb2 0xae	GEORGIAN CAPITAL LETTER XAN
					*p = 0x10EE;	//	ხ	0xe1 0x83 0xae	GEORGIAN SMALL LETTER XAN
					break;
				case 0x1CAF:		//	Ჯ	0xe1 0xb2 0xaf	GEORGIAN CAPITAL LETTER JHAN
					*p = 0x10EF;	//	ჯ	0xe1 0x83 0xaf	GEORGIAN SMALL LETTER JHAN
					break;
				case 0x1CB0:		//	Ჰ	0xe1 0xb2 0xb0	GEORGIAN CAPITAL LETTER HAE
					*p = 0x10F0;	//	ჰ	0xe1 0x83 0xb0	GEORGIAN SMALL LETTER HAE
					break;
				case 0x1CB1:		//	Ჱ	0xe1 0xb2 0xb1	GEORGIAN CAPITAL LETTER HE
					*p = 0x10F1;	//	ჱ	0xe1 0x83 0xb1	GEORGIAN SMALL LETTER HE
					break;
				case 0x1CB2:		//	Ჲ	0xe1 0xb2 0xb2	GEORGIAN CAPITAL LETTER HIE
					*p = 0x10F2;	//	ჲ	0xe1 0x83 0xb2	GEORGIAN SMALL LETTER HIE
					break;
				case 0x1CB3:		//	Ჳ	0xe1 0xb2 0xb3	GEORGIAN CAPITAL LETTER WE
					*p = 0x10F3;	//	ჳ	0xe1 0x83 0xb3	GEORGIAN SMALL LETTER WE
					break;
				case 0x1CB4:		//	Ჴ	0xe1 0xb2 0xb4	GEORGIAN CAPITAL LETTER HAR
					*p = 0x10F4;	//	ჴ	0xe1 0x83 0xb4	GEORGIAN SMALL LETTER HAR
					break;
				case 0x1CB5:		//	Ჵ	0xe1 0xb2 0xb5	GEORGIAN CAPITAL LETTER HOE
					*p = 0x10F5;	//	ჵ	0xe1 0x83 0xb5	GEORGIAN SMALL LETTER HOE
					break;
				case 0x1CB6:		//	Ჶ	0xe1 0xb2 0xb6	GEORGIAN CAPITAL LETTER FI
					*p = 0x10F6;	//	ჶ	0xe1 0x83 0xb6	GEORGIAN SMALL LETTER FI
					break;
				case 0x1CB7:		//	Ჷ	0xe1 0xb2 0xb7	GEORGIAN CAPITAL LETTER YN
					*p = 0x10F7;	//	ჷ	0xe1 0x83 0xb7	GEORGIAN SMALL LETTER YN
					break;
				case 0x1CB8:		//	Ჸ	0xe1 0xb2 0xb8	GEORGIAN CAPITAL LETTER ELIFI
					*p = 0x10F8;	//	ჸ	0xe1 0x83 0xb8	GEORGIAN SMALL LETTER ELIFI
					break;
				case 0x1CB9:		//	Ჹ	0xe1 0xb2 0xb9	GEORGIAN CAPITAL LETTER TURNED GAN
					*p = 0x10F9;	//	ჹ	0xe1 0x83 0xb9	GEORGIAN SMALL LETTER TURNED GAN
					break;
				case 0x1CBA:		//	Ჺ	0xe1 0xb2 0xba	GEORGIAN CAPITAL LETTER AIN
					*p = 0x10FA;	//	ჺ	0xe1 0x83 0xba	GEORGIAN SMALL LETTER AIN
					break;
				case 0x1CBD:		//	Ჽ	0xe1 0xb2 0xbd	GEORGIAN CAPITAL LETTER AEN
					*p = 0x10FD;	//	ჽ	0xe1 0x83 0xbd	GEORGIAN SMALL LETTER AEN
					break;
				case 0x1CBE:		//	Ჾ	0xe1 0xb2 0xbe	GEORGIAN CAPITAL LETTER HARD SIGN
					*p = 0x10FE;	//	ჾ	0xe1 0x83 0xbe	GEORGIAN SMALL LETTER HARD SIGN
					break;
				case 0x1CBF:		//	Ჿ	0xe1 0xb2 0xbf	GEORGIAN CAPITAL LETTER LABIAL SIGN
					*p = 0x10FF;	//	ჿ	0xe1 0x83 0xbf	GEORGIAN SMALL LETTER LABIAL SIGN
					break;
				case 0x1E00:		//	Ḁ	0xe1 0xb8 0x80	LATIN CAPITAL LETTER A WITH RING BELOW
					*p = 0x1E01;	//	ḁ	0xe1 0xb8 0x81	LATIN SMALL LETTER A WITH RING BELOW
					break;
				case 0x1E02:		//	Ḃ	0xe1 0xb8 0x82	LATIN CAPITAL LETTER B WITH DOT ABOVE
					*p = 0x1E03;	//	ḃ	0xe1 0xb8 0x83	LATIN SMALL LETTER B WITH DOT ABOVE
					break;
				case 0x1E04:		//	Ḅ	0xe1 0xb8 0x84	LATIN CAPITAL LETTER B WITH DOT BELOW
					*p = 0x1E05;	//	ḅ	0xe1 0xb8 0x85	LATIN SMALL LETTER B WITH DOT BELOW
					break;
				case 0x1E06:		//	Ḇ	0xe1 0xb8 0x86	LATIN CAPITAL LETTER B WITH LINE BELOW
					*p = 0x1E07;	//	ḇ	0xe1 0xb8 0x87	LATIN SMALL LETTER B WITH LINE BELOW
					break;
				case 0x1E08:		//	Ḉ	0xe1 0xb8 0x88	LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE
					*p = 0x1E09;	//	ḉ	0xe1 0xb8 0x89	LATIN SMALL LETTER C WITH CEDILLA AND ACUTE
					break;
				case 0x1E0A:		//	Ḋ	0xe1 0xb8 0x8a	LATIN CAPITAL LETTER D WITH DOT ABOVE
					*p = 0x1E0B;	//	ḋ	0xe1 0xb8 0x8b	LATIN SMALL LETTER D WITH DOT ABOVE
					break;
				case 0x1E0C:		//	Ḍ	0xe1 0xb8 0x8c	LATIN CAPITAL LETTER D WITH DOT BELOW
					*p = 0x1E0D;	//	ḍ	0xe1 0xb8 0x8d	LATIN SMALL LETTER D WITH DOT BELOW
					break;
				case 0x1E0E:		//	Ḏ	0xe1 0xb8 0x8e	LATIN CAPITAL LETTER D WITH LINE BELOW
					*p = 0x1E0F;	//	ḏ	0xe1 0xb8 0x8f	LATIN SMALL LETTER D WITH LINE BELOW
					break;
				case 0x1E10:		//	Ḑ	0xe1 0xb8 0x90	LATIN CAPITAL LETTER D WITH CEDILLA
					*p = 0x1E11;	//	ḑ	0xe1 0xb8 0x91	LATIN SMALL LETTER D WITH CEDILLA
					break;
				case 0x1E12:		//	Ḓ	0xe1 0xb8 0x92	LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW
					*p = 0x1E13;	//	ḓ	0xe1 0xb8 0x93	LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW
					break;
				case 0x1E14:		//	Ḕ	0xe1 0xb8 0x94	LATIN CAPITAL LETTER E WITH MACRON AND GRAVE
					*p = 0x1E15;	//	ḕ	0xe1 0xb8 0x95	LATIN SMALL LETTER E WITH MACRON AND GRAVE
					break;
				case 0x1E16:		//	Ḗ	0xe1 0xb8 0x96	LATIN CAPITAL LETTER E WITH MACRON AND ACUTE
					*p = 0x1E17;	//	ḗ	0xe1 0xb8 0x97	LATIN SMALL LETTER E WITH MACRON AND ACUTE
					break;
				case 0x1E18:		//	Ḙ	0xe1 0xb8 0x98	LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW
					*p = 0x1E19;	//	ḙ	0xe1 0xb8 0x99	LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW
					break;
				case 0x1E1A:		//	Ḛ	0xe1 0xb8 0x9a	LATIN CAPITAL LETTER E WITH TILDE BELOW
					*p = 0x1E1B;	//	ḛ	0xe1 0xb8 0x9b	LATIN SMALL LETTER E WITH TILDE BELOW
					break;
				case 0x1E1C:		//	Ḝ	0xe1 0xb8 0x9c	LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE
					*p = 0x1E1D;	//	ḝ	0xe1 0xb8 0x9d	LATIN SMALL LETTER E WITH CEDILLA AND BREVE
					break;
				case 0x1E1E:		//	Ḟ	0xe1 0xb8 0x9e	LATIN CAPITAL LETTER F WITH DOT ABOVE
					*p = 0x1E1F;	//	ḟ	0xe1 0xb8 0x9f	LATIN SMALL LETTER F WITH DOT ABOVE
					break;
				case 0x1E20:		//	Ḡ	0xe1 0xb8 0xa0	LATIN CAPITAL LETTER G WITH MACRON
					*p = 0x1E21;	//	ḡ	0xe1 0xb8 0xa1	LATIN SMALL LETTER G WITH MACRON
					break;
				case 0x1E22:		//	Ḣ	0xe1 0xb8 0xa2	LATIN CAPITAL LETTER H WITH DOT ABOVE
					*p = 0x1E23;	//	ḣ	0xe1 0xb8 0xa3	LATIN SMALL LETTER H WITH DOT ABOVE
					break;
				case 0x1E24:		//	Ḥ	0xe1 0xb8 0xa4	LATIN CAPITAL LETTER H WITH DOT BELOW
					*p = 0x1E25;	//	ḥ	0xe1 0xb8 0xa5	LATIN SMALL LETTER H WITH DOT BELOW
					break;
				case 0x1E26:		//	Ḧ	0xe1 0xb8 0xa6	LATIN CAPITAL LETTER H WITH DIAERESIS
					*p = 0x1E27;	//	ḧ	0xe1 0xb8 0xa7	LATIN SMALL LETTER H WITH DIAERESIS
					break;
				case 0x1E28:		//	Ḩ	0xe1 0xb8 0xa8	LATIN CAPITAL LETTER H WITH CEDILLA
					*p = 0x1E29;	//	ḩ	0xe1 0xb8 0xa9	LATIN SMALL LETTER H WITH CEDILLA
					break;
				case 0x1E2A:		//	Ḫ	0xe1 0xb8 0xaa	LATIN CAPITAL LETTER H WITH BREVE BELOW
					*p = 0x1E2B;	//	ḫ	0xe1 0xb8 0xab	LATIN SMALL LETTER H WITH BREVE BELOW
					break;
				case 0x1E2C:		//	Ḭ	0xe1 0xb8 0xac	LATIN CAPITAL LETTER I WITH TILDE BELOW
					*p = 0x1E2D;	//	ḭ	0xe1 0xb8 0xad	LATIN SMALL LETTER I WITH TILDE BELOW
					break;
				case 0x1E2E:		//	Ḯ	0xe1 0xb8 0xae	LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE
					*p = 0x1E2F;	//	ḯ	0xe1 0xb8 0xaf	LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE
					break;
				case 0x1E30:		//	Ḱ	0xe1 0xb8 0xb0	LATIN CAPITAL LETTER K WITH ACUTE
					*p = 0x1E31;	//	ḱ	0xe1 0xb8 0xb1	LATIN SMALL LETTER K WITH ACUTE
					break;
				case 0x1E32:		//	Ḳ	0xe1 0xb8 0xb2	LATIN CAPITAL LETTER K WITH DOT BELOW
					*p = 0x1E33;	//	ḳ	0xe1 0xb8 0xb3	LATIN SMALL LETTER K WITH DOT BELOW
					break;
				case 0x1E34:		//	Ḵ	0xe1 0xb8 0xb4	LATIN CAPITAL LETTER K WITH LINE BELOW
					*p = 0x1E35;	//	ḵ	0xe1 0xb8 0xb5	LATIN SMALL LETTER K WITH LINE BELOW
					break;
				case 0x1E36:		//	Ḷ	0xe1 0xb8 0xb6	LATIN CAPITAL LETTER L WITH DOT BELOW
					*p = 0x1E37;	//	ḷ	0xe1 0xb8 0xb7	LATIN SMALL LETTER L WITH DOT BELOW
					break;
				case 0x1E38:		//	Ḹ	0xe1 0xb8 0xb8	LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON
					*p = 0x1E39;	//	ḹ	0xe1 0xb8 0xb9	LATIN SMALL LETTER L WITH DOT BELOW AND MACRON
					break;
				case 0x1E3A:		//	Ḻ	0xe1 0xb8 0xba	LATIN CAPITAL LETTER L WITH LINE BELOW
					*p = 0x1E3B;	//	ḻ	0xe1 0xb8 0xbb	LATIN SMALL LETTER L WITH LINE BELOW
					break;
				case 0x1E3C:		//	Ḽ	0xe1 0xb8 0xbc	LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW
					*p = 0x1E3D;	//	ḽ	0xe1 0xb8 0xbd	LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW
					break;
				case 0x1E3E:		//	Ḿ	0xe1 0xb8 0xbe	LATIN CAPITAL LETTER M WITH ACUTE
					*p = 0x1E3F;	//	ḿ	0xe1 0xb8 0xbf	LATIN SMALL LETTER M WITH ACUTE
					break;
				case 0x1E40:		//	Ṁ	0xe1 0xb9 0x80	LATIN CAPITAL LETTER M WITH DOT ABOVE
					*p = 0x1E41;	//	ṁ	0xe1 0xb9 0x81	LATIN SMALL LETTER M WITH DOT ABOVE
					break;
				case 0x1E42:		//	Ṃ	0xe1 0xb9 0x82	LATIN CAPITAL LETTER M WITH DOT BELOW
					*p = 0x1E43;	//	ṃ	0xe1 0xb9 0x83	LATIN SMALL LETTER M WITH DOT BELOW
					break;
				case 0x1E44:		//	Ṅ	0xe1 0xb9 0x84	LATIN CAPITAL LETTER N WITH DOT ABOVE
					*p = 0x1E45;	//	ṅ	0xe1 0xb9 0x85	LATIN SMALL LETTER N WITH DOT ABOVE
					break;
				case 0x1E46:		//	Ṇ	0xe1 0xb9 0x86	LATIN CAPITAL LETTER N WITH DOT BELOW
					*p = 0x1E47;	//	ṇ	0xe1 0xb9 0x87	LATIN SMALL LETTER N WITH DOT BELOW
					break;
				case 0x1E48:		//	Ṉ	0xe1 0xb9 0x88	LATIN CAPITAL LETTER N WITH LINE BELOW
					*p = 0x1E49;	//	ṉ	0xe1 0xb9 0x89	LATIN SMALL LETTER N WITH LINE BELOW
					break;
				case 0x1E4A:		//	Ṋ	0xe1 0xb9 0x8a	LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW
					*p = 0x1E4B;	//	ṋ	0xe1 0xb9 0x8b	LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW
					break;
				case 0x1E4C:		//	Ṍ	0xe1 0xb9 0x8c	LATIN CAPITAL LETTER O WITH TILDE AND ACUTE
					*p = 0x1E4D;	//	ṍ	0xe1 0xb9 0x8d	LATIN SMALL LETTER O WITH TILDE AND ACUTE
					break;
				case 0x1E4E:		//	Ṏ	0xe1 0xb9 0x8e	LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS
					*p = 0x1E4F;	//	ṏ	0xe1 0xb9 0x8f	LATIN SMALL LETTER O WITH TILDE AND DIAERESIS
					break;
				case 0x1E50:		//	Ṑ	0xe1 0xb9 0x90	LATIN CAPITAL LETTER O WITH MACRON AND GRAVE
					*p = 0x1E51;	//	ṑ	0xe1 0xb9 0x91	LATIN SMALL LETTER O WITH MACRON AND GRAVE
					break;
				case 0x1E52:		//	Ṓ	0xe1 0xb9 0x92	LATIN CAPITAL LETTER O WITH MACRON AND ACUTE
					*p = 0x1E53;	//	ṓ	0xe1 0xb9 0x93	LATIN SMALL LETTER O WITH MACRON AND ACUTE
					break;
				case 0x1E54:		//	Ṕ	0xe1 0xb9 0x94	LATIN CAPITAL LETTER P WITH ACUTE
					*p = 0x1E55;	//	ṕ	0xe1 0xb9 0x95	LATIN SMALL LETTER P WITH ACUTE
					break;
				case 0x1E56:		//	Ṗ	0xe1 0xb9 0x96	LATIN CAPITAL LETTER P WITH DOT ABOVE
					*p = 0x1E57;	//	ṗ	0xe1 0xb9 0x97	LATIN SMALL LETTER P WITH DOT ABOVE
					break;
				case 0x1E58:		//	Ṙ	0xe1 0xb9 0x98	LATIN CAPITAL LETTER R WITH DOT ABOVE
					*p = 0x1E59;	//	ṙ	0xe1 0xb9 0x99	LATIN SMALL LETTER R WITH DOT ABOVE
					break;
				case 0x1E5A:		//	Ṛ	0xe1 0xb9 0x9a	LATIN CAPITAL LETTER R WITH DOT BELOW
					*p = 0x1E5B;	//	ṛ	0xe1 0xb9 0x9b	LATIN SMALL LETTER R WITH DOT BELOW
					break;
				case 0x1E5C:		//	Ṝ	0xe1 0xb9 0x9c	LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON
					*p = 0x1E5D;	//	ṝ	0xe1 0xb9 0x9d	LATIN SMALL LETTER R WITH DOT BELOW AND MACRON
					break;
				case 0x1E5E:		//	Ṟ	0xe1 0xb9 0x9e	LATIN CAPITAL LETTER R WITH LINE BELOW
					*p = 0x1E5F;	//	ṟ	0xe1 0xb9 0x9f	LATIN SMALL LETTER R WITH LINE BELOW
					break;
				case 0x1E60:		//	Ṡ	0xe1 0xb9 0xa0	LATIN CAPITAL LETTER S WITH DOT ABOVE
					*p = 0x1E61;	//	ṡ	0xe1 0xb9 0xa1	LATIN SMALL LETTER S WITH DOT ABOVE
					break;
				case 0x1E62:		//	Ṣ	0xe1 0xb9 0xa2	LATIN CAPITAL LETTER S WITH DOT BELOW
					*p = 0x1E63;	//	ṣ	0xe1 0xb9 0xa3	LATIN SMALL LETTER S WITH DOT BELOW
					break;
				case 0x1E64:		//	Ṥ	0xe1 0xb9 0xa4	LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE
					*p = 0x1E65;	//	ṥ	0xe1 0xb9 0xa5	LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE
					break;
				case 0x1E66:		//	Ṧ	0xe1 0xb9 0xa6	LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE
					*p = 0x1E67;	//	ṧ	0xe1 0xb9 0xa7	LATIN SMALL LETTER S WITH CARON AND DOT ABOVE
					break;
				case 0x1E68:		//	Ṩ	0xe1 0xb9 0xa8	LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE
					*p = 0x1E69;	//	ṩ	0xe1 0xb9 0xa9	LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE
					break;
				case 0x1E6A:		//	Ṫ	0xe1 0xb9 0xaa	LATIN CAPITAL LETTER T WITH DOT ABOVE
					*p = 0x1E6B;	//	ṫ	0xe1 0xb9 0xab	LATIN SMALL LETTER T WITH DOT ABOVE
					break;
				case 0x1E6C:		//	Ṭ	0xe1 0xb9 0xac	LATIN CAPITAL LETTER T WITH DOT BELOW
					*p = 0x1E6D;	//	ṭ	0xe1 0xb9 0xad	LATIN SMALL LETTER T WITH DOT BELOW
					break;
				case 0x1E6E:		//	Ṯ	0xe1 0xb9 0xae	LATIN CAPITAL LETTER T WITH LINE BELOW
					*p = 0x1E6F;	//	ṯ	0xe1 0xb9 0xaf	LATIN SMALL LETTER T WITH LINE BELOW
					break;
				case 0x1E70:		//	Ṱ	0xe1 0xb9 0xb0	LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW
					*p = 0x1E71;	//	ṱ	0xe1 0xb9 0xb1	LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW
					break;
				case 0x1E72:		//	Ṳ	0xe1 0xb9 0xb2	LATIN CAPITAL LETTER U WITH DIAERESIS BELOW
					*p = 0x1E73;	//	ṳ	0xe1 0xb9 0xb3	LATIN SMALL LETTER U WITH DIAERESIS BELOW
					break;
				case 0x1E74:		//	Ṵ	0xe1 0xb9 0xb4	LATIN CAPITAL LETTER U WITH TILDE BELOW
					*p = 0x1E75;	//	ṵ	0xe1 0xb9 0xb5	LATIN SMALL LETTER U WITH TILDE BELOW
					break;
				case 0x1E76:		//	Ṷ	0xe1 0xb9 0xb6	LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW
					*p = 0x1E77;	//	ṷ	0xe1 0xb9 0xb7	LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW
					break;
				case 0x1E78:		//	Ṹ	0xe1 0xb9 0xb8	LATIN CAPITAL LETTER U WITH TILDE AND ACUTE
					*p = 0x1E79;	//	ṹ	0xe1 0xb9 0xb9	LATIN SMALL LETTER U WITH TILDE AND ACUTE
					break;
				case 0x1E7A:		//	Ṻ	0xe1 0xb9 0xba	LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS
					*p = 0x1E7B;	//	ṻ	0xe1 0xb9 0xbb	LATIN SMALL LETTER U WITH MACRON AND DIAERESIS
					break;
				case 0x1E7C:		//	Ṽ	0xe1 0xb9 0xbc	LATIN CAPITAL LETTER V WITH TILDE
					*p = 0x1E7D;	//	ṽ	0xe1 0xb9 0xbd	LATIN SMALL LETTER V WITH TILDE
					break;
				case 0x1E7E:		//	Ṿ	0xe1 0xb9 0xbe	LATIN CAPITAL LETTER V WITH DOT BELOW
					*p = 0x1E7F;	//	ṿ	0xe1 0xb9 0xbf	LATIN SMALL LETTER V WITH DOT BELOW
					break;
				case 0x1E80:		//	Ẁ	0xe1 0xba 0x80	LATIN CAPITAL LETTER W WITH GRAVE
					*p = 0x1E81;	//	ẁ	0xe1 0xba 0x81	LATIN SMALL LETTER W WITH GRAVE
					break;
				case 0x1E82:		//	Ẃ	0xe1 0xba 0x82	LATIN CAPITAL LETTER W WITH ACUTE
					*p = 0x1E83;	//	ẃ	0xe1 0xba 0x83	LATIN SMALL LETTER W WITH ACUTE
					break;
				case 0x1E84:		//	Ẅ	0xe1 0xba 0x84	LATIN CAPITAL LETTER W WITH DIAERESIS
					*p = 0x1E85;	//	ẅ	0xe1 0xba 0x85	LATIN SMALL LETTER W WITH DIAERESIS
					break;
				case 0x1E86:		//	Ẇ	0xe1 0xba 0x86	LATIN CAPITAL LETTER W WITH DOT ABOVE
					*p = 0x1E87;	//	ẇ	0xe1 0xba 0x87	LATIN SMALL LETTER W WITH DOT ABOVE
					break;
				case 0x1E88:		//	Ẉ	0xe1 0xba 0x88	LATIN CAPITAL LETTER W WITH DOT BELOW
					*p = 0x1E89;	//	ẉ	0xe1 0xba 0x89	LATIN SMALL LETTER W WITH DOT BELOW
					break;
				case 0x1E8A:		//	Ẋ	0xe1 0xba 0x8a	LATIN CAPITAL LETTER X WITH DOT ABOVE
					*p = 0x1E8B;	//	ẋ	0xe1 0xba 0x8b	LATIN SMALL LETTER X WITH DOT ABOVE
					break;
				case 0x1E8C:		//	Ẍ	0xe1 0xba 0x8c	LATIN CAPITAL LETTER X WITH DIAERESIS
					*p = 0x1E8D;	//	ẍ	0xe1 0xba 0x8d	LATIN SMALL LETTER X WITH DIAERESIS
					break;
				case 0x1E8E:		//	Ẏ	0xe1 0xba 0x8e	LATIN CAPITAL LETTER Y WITH DOT ABOVE
					*p = 0x1E8F;	//	ẏ	0xe1 0xba 0x8f	LATIN SMALL LETTER Y WITH DOT ABOVE
					break;
				case 0x1E90:		//	Ẑ	0xe1 0xba 0x90	LATIN CAPITAL LETTER Z WITH CIRCUMFLEX
					*p = 0x1E91;	//	ẑ	0xe1 0xba 0x91	LATIN SMALL LETTER Z WITH CIRCUMFLEX
					break;
				case 0x1E92:		//	Ẓ	0xe1 0xba 0x92	LATIN CAPITAL LETTER Z WITH DOT BELOW
					*p = 0x1E93;	//	ẓ	0xe1 0xba 0x93	LATIN SMALL LETTER Z WITH DOT BELOW
					break;
				case 0x1E94:		//	Ẕ	0xe1 0xba 0x94	LATIN CAPITAL LETTER Z WITH LINE BELOW
					*p = 0x1E95;	//	ẕ	0xe1 0xba 0x95	LATIN SMALL LETTER Z WITH LINE BELOW
					break;
				case 0x1E9E:		//	ẞ	0xe1 0xba 0x9e	LATIN CAPITAL LETTER SHARP S
					*p = 0x00DF;	//	ß	0xc3 0x9f	LATIN SMALL LETTER SHARP S
					break;
				case 0x1EA0:		//	Ạ	0xe1 0xba 0xa0	LATIN CAPITAL LETTER A WITH DOT BELOW
					*p = 0x1EA1;	//	ạ	0xe1 0xba 0xa1	LATIN SMALL LETTER A WITH DOT BELOW
					break;
				case 0x1EA2:		//	Ả	0xe1 0xba 0xa2	LATIN CAPITAL LETTER A WITH HOOK ABOVE
					*p = 0x1EA3;	//	ả	0xe1 0xba 0xa3	LATIN SMALL LETTER A WITH HOOK ABOVE
					break;
				case 0x1EA4:		//	Ấ	0xe1 0xba 0xa4	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE
					*p = 0x1EA5;	//	ấ	0xe1 0xba 0xa5	LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE
					break;
				case 0x1EA6:		//	Ầ	0xe1 0xba 0xa6	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE
					*p = 0x1EA7;	//	ầ	0xe1 0xba 0xa7	LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE
					break;
				case 0x1EA8:		//	Ẩ	0xe1 0xba 0xa8	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE
					*p = 0x1EA9;	//	ẩ	0xe1 0xba 0xa9	LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE
					break;
				case 0x1EAA:		//	Ẫ	0xe1 0xba 0xaa	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE
					*p = 0x1EAB;	//	ẫ	0xe1 0xba 0xab	LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE
					break;
				case 0x1EAC:		//	Ậ	0xe1 0xba 0xac	LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW
					*p = 0x1EAD;	//	ậ	0xe1 0xba 0xad	LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW
					break;
				case 0x1EAE:		//	Ắ	0xe1 0xba 0xae	LATIN CAPITAL LETTER A WITH BREVE AND ACUTE
					*p = 0x1EAF;	//	ắ	0xe1 0xba 0xaf	LATIN SMALL LETTER A WITH BREVE AND ACUTE
					break;
				case 0x1EB0:		//	Ằ	0xe1 0xba 0xb0	LATIN CAPITAL LETTER A WITH BREVE AND GRAVE
					*p = 0x1EB1;	//	ằ	0xe1 0xba 0xb1	LATIN SMALL LETTER A WITH BREVE AND GRAVE
					break;
				case 0x1EB2:		//	Ẳ	0xe1 0xba 0xb2	LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE
					*p = 0x1EB3;	//	ẳ	0xe1 0xba 0xb3	LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE
					break;
				case 0x1EB4:		//	Ẵ	0xe1 0xba 0xb4	LATIN CAPITAL LETTER A WITH BREVE AND TILDE
					*p = 0x1EB5;	//	ẵ	0xe1 0xba 0xb5	LATIN SMALL LETTER A WITH BREVE AND TILDE
					break;
				case 0x1EB6:		//	Ặ	0xe1 0xba 0xb6	LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW
					*p = 0x1EB7;	//	ặ	0xe1 0xba 0xb7	LATIN SMALL LETTER A WITH BREVE AND DOT BELOW
					break;
				case 0x1EB8:		//	Ẹ	0xe1 0xba 0xb8	LATIN CAPITAL LETTER E WITH DOT BELOW
					*p = 0x1EB9;	//	ẹ	0xe1 0xba 0xb9	LATIN SMALL LETTER E WITH DOT BELOW
					break;
				case 0x1EBA:		//	Ẻ	0xe1 0xba 0xba	LATIN CAPITAL LETTER E WITH HOOK ABOVE
					*p = 0x1EBB;	//	ẻ	0xe1 0xba 0xbb	LATIN SMALL LETTER E WITH HOOK ABOVE
					break;
				case 0x1EBC:		//	Ẽ	0xe1 0xba 0xbc	LATIN CAPITAL LETTER E WITH TILDE
					*p = 0x1EBD;	//	ẽ	0xe1 0xba 0xbd	LATIN SMALL LETTER E WITH TILDE
					break;
				case 0x1EBE:		//	Ế	0xe1 0xba 0xbe	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE
					*p = 0x1EBF;	//	ế	0xe1 0xba 0xbf	LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE
					break;
				case 0x1EC0:		//	Ề	0xe1 0xbb 0x80	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE
					*p = 0x1EC1;	//	ề	0xe1 0xbb 0x81	LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE
					break;
				case 0x1EC2:		//	Ể	0xe1 0xbb 0x82	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE
					*p = 0x1EC3;	//	ể	0xe1 0xbb 0x83	LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE
					break;
				case 0x1EC4:		//	Ễ	0xe1 0xbb 0x84	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE
					*p = 0x1EC5;	//	ễ	0xe1 0xbb 0x85	LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE
					break;
				case 0x1EC6:		//	Ệ	0xe1 0xbb 0x86	LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW
					*p = 0x1EC7;	//	ệ	0xe1 0xbb 0x87	LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW
					break;
				case 0x1EC8:		//	Ỉ	0xe1 0xbb 0x88	LATIN CAPITAL LETTER I WITH HOOK ABOVE
					*p = 0x1EC9;	//	ỉ	0xe1 0xbb 0x89	LATIN SMALL LETTER I WITH HOOK ABOVE
					break;
				case 0x1ECA:		//	Ị	0xe1 0xbb 0x8a	LATIN CAPITAL LETTER I WITH DOT BELOW
					*p = 0x1ECB;	//	ị	0xe1 0xbb 0x8b	LATIN SMALL LETTER I WITH DOT BELOW
					break;
				case 0x1ECC:		//	Ọ	0xe1 0xbb 0x8c	LATIN CAPITAL LETTER O WITH DOT BELOW
					*p = 0x1ECD;	//	ọ	0xe1 0xbb 0x8d	LATIN SMALL LETTER O WITH DOT BELOW
					break;
				case 0x1ECE:		//	Ỏ	0xe1 0xbb 0x8e	LATIN CAPITAL LETTER O WITH HOOK ABOVE
					*p = 0x1ECF;	//	ỏ	0xe1 0xbb 0x8f	LATIN SMALL LETTER O WITH HOOK ABOVE
					break;
				case 0x1ED0:		//	Ố	0xe1 0xbb 0x90	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE
					*p = 0x1ED1;	//	ố	0xe1 0xbb 0x91	LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE
					break;
				case 0x1ED2:		//	Ồ	0xe1 0xbb 0x92	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE
					*p = 0x1ED3;	//	ồ	0xe1 0xbb 0x93	LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE
					break;
				case 0x1ED4:		//	Ổ	0xe1 0xbb 0x94	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE
					*p = 0x1ED5;	//	ổ	0xe1 0xbb 0x95	LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE
					break;
				case 0x1ED6:		//	Ỗ	0xe1 0xbb 0x96	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE
					*p = 0x1ED7;	//	ỗ	0xe1 0xbb 0x97	LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE
					break;
				case 0x1ED8:		//	Ộ	0xe1 0xbb 0x98	LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW
					*p = 0x1ED9;	//	ộ	0xe1 0xbb 0x99	LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW
					break;
				case 0x1EDA:		//	Ớ	0xe1 0xbb 0x9a	LATIN CAPITAL LETTER O WITH HORN AND ACUTE
					*p = 0x1EDB;	//	ớ	0xe1 0xbb 0x9b	LATIN SMALL LETTER O WITH HORN AND ACUTE
					break;
				case 0x1EDC:		//	Ờ	0xe1 0xbb 0x9c	LATIN CAPITAL LETTER O WITH HORN AND GRAVE
					*p = 0x1EDD;	//	ờ	0xe1 0xbb 0x9d	LATIN SMALL LETTER O WITH HORN AND GRAVE
					break;
				case 0x1EDE:		//	Ở	0xe1 0xbb 0x9e	LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE
					*p = 0x1EDF;	//	ở	0xe1 0xbb 0x9f	LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE
					break;
				case 0x1EE0:		//	Ỡ	0xe1 0xbb 0xa0	LATIN CAPITAL LETTER O WITH HORN AND TILDE
					*p = 0x1EE1;	//	ỡ	0xe1 0xbb 0xa1	LATIN SMALL LETTER O WITH HORN AND TILDE
					break;
				case 0x1EE2:		//	Ợ	0xe1 0xbb 0xa2	LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW
					*p = 0x1EE3;	//	ợ	0xe1 0xbb 0xa3	LATIN SMALL LETTER O WITH HORN AND DOT BELOW
					break;
				case 0x1EE4:		//	Ụ	0xe1 0xbb 0xa4	LATIN CAPITAL LETTER U WITH DOT BELOW
					*p = 0x1EE5;	//	ụ	0xe1 0xbb 0xa5	LATIN SMALL LETTER U WITH DOT BELOW
					break;
				case 0x1EE6:		//	Ủ	0xe1 0xbb 0xa6	LATIN CAPITAL LETTER U WITH HOOK ABOVE
					*p = 0x1EE7;	//	ủ	0xe1 0xbb 0xa7	LATIN SMALL LETTER U WITH HOOK ABOVE
					break;
				case 0x1EE8:		//	Ứ	0xe1 0xbb 0xa8	LATIN CAPITAL LETTER U WITH HORN AND ACUTE
					*p = 0x1EE9;	//	ứ	0xe1 0xbb 0xa9	LATIN SMALL LETTER U WITH HORN AND ACUTE
					break;
				case 0x1EEA:		//	Ừ	0xe1 0xbb 0xaa	LATIN CAPITAL LETTER U WITH HORN AND GRAVE
					*p = 0x1EEB;	//	ừ	0xe1 0xbb 0xab	LATIN SMALL LETTER U WITH HORN AND GRAVE
					break;
				case 0x1EEC:		//	Ử	0xe1 0xbb 0xac	LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE
					*p = 0x1EED;	//	ử	0xe1 0xbb 0xad	LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE
					break;
				case 0x1EEE:		//	Ữ	0xe1 0xbb 0xae	LATIN CAPITAL LETTER U WITH HORN AND TILDE
					*p = 0x1EEF;	//	ữ	0xe1 0xbb 0xaf	LATIN SMALL LETTER U WITH HORN AND TILDE
					break;
				case 0x1EF0:		//	Ự	0xe1 0xbb 0xb0	LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW
					*p = 0x1EF1;	//	ự	0xe1 0xbb 0xb1	LATIN SMALL LETTER U WITH HORN AND DOT BELOW
					break;
				case 0x1EF2:		//	Ỳ	0xe1 0xbb 0xb2	LATIN CAPITAL LETTER Y WITH GRAVE
					*p = 0x1EF3;	//	ỳ	0xe1 0xbb 0xb3	LATIN SMALL LETTER Y WITH GRAVE
					break;
				case 0x1EF4:		//	Ỵ	0xe1 0xbb 0xb4	LATIN CAPITAL LETTER Y WITH DOT BELOW
					*p = 0x1EF5;	//	ỵ	0xe1 0xbb 0xb5	LATIN SMALL LETTER Y WITH DOT BELOW
					break;
				case 0x1EF6:		//	Ỷ	0xe1 0xbb 0xb6	LATIN CAPITAL LETTER Y WITH HOOK ABOVE
					*p = 0x1EF7;	//	ỷ	0xe1 0xbb 0xb7	LATIN SMALL LETTER Y WITH HOOK ABOVE
					break;
				case 0x1EF8:		//	Ỹ	0xe1 0xbb 0xb8	LATIN CAPITAL LETTER Y WITH TILDE
					*p = 0x1EF9;	//	ỹ	0xe1 0xbb 0xb9	LATIN SMALL LETTER Y WITH TILDE
					break;
				case 0x1EFA:		//	Ỻ	0xe1 0xbb 0xba	LATIN CAPITAL LETTER MIDDLE-WELSH LL
					*p = 0x1EFB;	//	ỻ	0xe1 0xbb 0xbb	LATIN SMALL LETTER MIDDLE-WELSH LL
					break;
				case 0x1EFC:		//	Ỽ	0xe1 0xbb 0xbc	LATIN CAPITAL LETTER MIDDLE-WELSH V
					*p = 0x1EFD;	//	ỽ	0xe1 0xbb 0xbd	LATIN SMALL LETTER MIDDLE-WELSH V
					break;
				case 0x1EFE:		//	Ỿ	0xe1 0xbb 0xbe	LATIN CAPITAL LETTER Y WITH LOOP
					*p = 0x1EFF;	//	ỿ	0xe1 0xbb 0xbf	LATIN SMALL LETTER Y WITH LOOP
					break;
				case 0x1F08:		//	Ἀ	0xe1 0xbc 0x88	GREEK CAPITAL LETTER ALPHA WITH PSILI
					*p = 0x1F00;	//	ἀ	0xe1 0xbc 0x80	GREEK SMALL LETTER ALPHA WITH PSILI
					break;
				case 0x1F09:		//	Ἁ	0xe1 0xbc 0x89	GREEK CAPITAL LETTER ALPHA WITH DASIA
					*p = 0x1F01;	//	ἁ	0xe1 0xbc 0x81	GREEK SMALL LETTER ALPHA WITH DASIA
					break;
				case 0x1F0A:		//	Ἂ	0xe1 0xbc 0x8a	GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA
					*p = 0x1F02;	//	ἂ	0xe1 0xbc 0x82	GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA
					break;
				case 0x1F0B:		//	Ἃ	0xe1 0xbc 0x8b	GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA
					*p = 0x1F03;	//	ἃ	0xe1 0xbc 0x83	GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA
					break;
				case 0x1F0C:		//	Ἄ	0xe1 0xbc 0x8c	GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA
					*p = 0x1F04;	//	ἄ	0xe1 0xbc 0x84	GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA
					break;
				case 0x1F0D:		//	Ἅ	0xe1 0xbc 0x8d	GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA
					*p = 0x1F05;	//	ἅ	0xe1 0xbc 0x85	GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA
					break;
				case 0x1F0E:		//	Ἆ	0xe1 0xbc 0x8e	GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI
					*p = 0x1F06;	//	ἆ	0xe1 0xbc 0x86	GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F0F:		//	Ἇ	0xe1 0xbc 0x8f	GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI
					*p = 0x1F07;	//	ἇ	0xe1 0xbc 0x87	GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F18:		//	Ἐ	0xe1 0xbc 0x98	GREEK CAPITAL LETTER EPSILON WITH PSILI
					*p = 0x1F10;	//	ἐ	0xe1 0xbc 0x90	GREEK SMALL LETTER EPSILON WITH PSILI
					break;
				case 0x1F19:		//	Ἑ	0xe1 0xbc 0x99	GREEK CAPITAL LETTER EPSILON WITH DASIA
					*p = 0x1F11;	//	ἑ	0xe1 0xbc 0x91	GREEK SMALL LETTER EPSILON WITH DASIA
					break;
				case 0x1F1A:		//	Ἒ	0xe1 0xbc 0x9a	GREEK CAPITAL LETTER EPSILON WITH PSILI AND VARIA
					*p = 0x1F12;	//	ἒ	0xe1 0xbc 0x92	GREEK SMALL LETTER EPSILON WITH PSILI AND VARIA
					break;
				case 0x1F1B:		//	Ἓ	0xe1 0xbc 0x9b	GREEK CAPITAL LETTER EPSILON WITH DASIA AND VARIA
					*p = 0x1F13;	//	ἓ	0xe1 0xbc 0x93	GREEK SMALL LETTER EPSILON WITH DASIA AND VARIA
					break;
				case 0x1F1C:		//	Ἔ	0xe1 0xbc 0x9c	GREEK CAPITAL LETTER EPSILON WITH PSILI AND OXIA
					*p = 0x1F14;	//	ἔ	0xe1 0xbc 0x94	GREEK SMALL LETTER EPSILON WITH PSILI AND OXIA
					break;
				case 0x1F1D:		//	Ἕ	0xe1 0xbc 0x9d	GREEK CAPITAL LETTER EPSILON WITH DASIA AND OXIA
					*p = 0x1F15;	//	ἕ	0xe1 0xbc 0x95	GREEK SMALL LETTER EPSILON WITH DASIA AND OXIA
					break;
				case 0x1F28:		//	Ἠ	0xe1 0xbc 0xa8	GREEK CAPITAL LETTER ETA WITH PSILI
					*p = 0x1F20;	//	ἠ	0xe1 0xbc 0xa0	GREEK SMALL LETTER ETA WITH PSILI
					break;
				case 0x1F29:		//	Ἡ	0xe1 0xbc 0xa9	GREEK CAPITAL LETTER ETA WITH DASIA
					*p = 0x1F21;	//	ἡ	0xe1 0xbc 0xa1	GREEK SMALL LETTER ETA WITH DASIA
					break;
				case 0x1F2A:		//	Ἢ	0xe1 0xbc 0xaa	GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA
					*p = 0x1F22;	//	ἢ	0xe1 0xbc 0xa2	GREEK SMALL LETTER ETA WITH PSILI AND VARIA
					break;
				case 0x1F2B:		//	Ἣ	0xe1 0xbc 0xab	GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA
					*p = 0x1F23;	//	ἣ	0xe1 0xbc 0xa3	GREEK SMALL LETTER ETA WITH DASIA AND VARIA
					break;
				case 0x1F2C:		//	Ἤ	0xe1 0xbc 0xac	GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA
					*p = 0x1F24;	//	ἤ	0xe1 0xbc 0xa4	GREEK SMALL LETTER ETA WITH PSILI AND OXIA
					break;
				case 0x1F2D:		//	Ἥ	0xe1 0xbc 0xad	GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA
					*p = 0x1F25;	//	ἥ	0xe1 0xbc 0xa5	GREEK SMALL LETTER ETA WITH DASIA AND OXIA
					break;
				case 0x1F2E:		//	Ἦ	0xe1 0xbc 0xae	GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI
					*p = 0x1F26;	//	ἦ	0xe1 0xbc 0xa6	GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F2F:		//	Ἧ	0xe1 0xbc 0xaf	GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI
					*p = 0x1F27;	//	ἧ	0xe1 0xbc 0xa7	GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F38:		//	Ἰ	0xe1 0xbc 0xb8	GREEK CAPITAL LETTER IOTA WITH PSILI
					*p = 0x1F30;	//	ἰ	0xe1 0xbc 0xb0	GREEK SMALL LETTER IOTA WITH PSILI
					break;
				case 0x1F39:		//	Ἱ	0xe1 0xbc 0xb9	GREEK CAPITAL LETTER IOTA WITH DASIA
					*p = 0x1F31;	//	ἱ	0xe1 0xbc 0xb1	GREEK SMALL LETTER IOTA WITH DASIA
					break;
				case 0x1F3A:		//	Ἲ	0xe1 0xbc 0xba	GREEK CAPITAL LETTER IOTA WITH PSILI AND VARIA
					*p = 0x1F32;	//	ἲ	0xe1 0xbc 0xb2	GREEK SMALL LETTER IOTA WITH PSILI AND VARIA
					break;
				case 0x1F3B:		//	Ἳ	0xe1 0xbc 0xbb	GREEK CAPITAL LETTER IOTA WITH DASIA AND VARIA
					*p = 0x1F33;	//	ἳ	0xe1 0xbc 0xb3	GREEK SMALL LETTER IOTA WITH DASIA AND VARIA
					break;
				case 0x1F3C:		//	Ἴ	0xe1 0xbc 0xbc	GREEK CAPITAL LETTER IOTA WITH PSILI AND OXIA
					*p = 0x1F34;	//	ἴ	0xe1 0xbc 0xb4	GREEK SMALL LETTER IOTA WITH PSILI AND OXIA
					break;
				case 0x1F3D:		//	Ἵ	0xe1 0xbc 0xbd	GREEK CAPITAL LETTER IOTA WITH DASIA AND OXIA
					*p = 0x1F35;	//	ἵ	0xe1 0xbc 0xb5	GREEK SMALL LETTER IOTA WITH DASIA AND OXIA
					break;
				case 0x1F3E:		//	Ἶ	0xe1 0xbc 0xbe	GREEK CAPITAL LETTER IOTA WITH PSILI AND PERISPOMENI
					*p = 0x1F36;	//	ἶ	0xe1 0xbc 0xb6	GREEK SMALL LETTER IOTA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F3F:		//	Ἷ	0xe1 0xbc 0xbf	GREEK CAPITAL LETTER IOTA WITH DASIA AND PERISPOMENI
					*p = 0x1F37;	//	ἷ	0xe1 0xbc 0xb7	GREEK SMALL LETTER IOTA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F48:		//	Ὀ	0xe1 0xbd 0x88	GREEK CAPITAL LETTER OMICRON WITH PSILI
					*p = 0x1F40;	//	ὀ	0xe1 0xbd 0x80	GREEK SMALL LETTER OMICRON WITH PSILI
					break;
				case 0x1F49:		//	Ὁ	0xe1 0xbd 0x89	GREEK CAPITAL LETTER OMICRON WITH DASIA
					*p = 0x1F41;	//	ὁ	0xe1 0xbd 0x81	GREEK SMALL LETTER OMICRON WITH DASIA
					break;
				case 0x1F4A:		//	Ὂ	0xe1 0xbd 0x8a	GREEK CAPITAL LETTER OMICRON WITH PSILI AND VARIA
					*p = 0x1F42;	//	ὂ	0xe1 0xbd 0x82	GREEK SMALL LETTER OMICRON WITH PSILI AND VARIA
					break;
				case 0x1F4B:		//	Ὃ	0xe1 0xbd 0x8b	GREEK CAPITAL LETTER OMICRON WITH DASIA AND VARIA
					*p = 0x1F43;	//	ὃ	0xe1 0xbd 0x83	GREEK SMALL LETTER OMICRON WITH DASIA AND VARIA
					break;
				case 0x1F4C:		//	Ὄ	0xe1 0xbd 0x8c	GREEK CAPITAL LETTER OMICRON WITH PSILI AND OXIA
					*p = 0x1F44;	//	ὄ	0xe1 0xbd 0x84	GREEK SMALL LETTER OMICRON WITH PSILI AND OXIA
					break;
				case 0x1F4D:		//	Ὅ	0xe1 0xbd 0x8d	GREEK CAPITAL LETTER OMICRON WITH DASIA AND OXIA
					*p = 0x1F45;	//	ὅ	0xe1 0xbd 0x85	GREEK SMALL LETTER OMICRON WITH DASIA AND OXIA
					break;
				case 0x1F59:		//	Ὑ	0xe1 0xbd 0x99	GREEK CAPITAL LETTER UPSILON WITH DASIA
					*p = 0x1F51;	//	ὑ	0xe1 0xbd 0x91	GREEK SMALL LETTER UPSILON WITH DASIA
					break;
				case 0x1F5B:		//	Ὓ	0xe1 0xbd 0x9b	GREEK CAPITAL LETTER UPSILON WITH DASIA AND VARIA
					*p = 0x1F53;	//	ὓ	0xe1 0xbd 0x93	GREEK SMALL LETTER UPSILON WITH DASIA AND VARIA
					break;
				case 0x1F5D:		//	Ὕ	0xe1 0xbd 0x9d	GREEK CAPITAL LETTER UPSILON WITH DASIA AND OXIA
					*p = 0x1F55;	//	ὕ	0xe1 0xbd 0x95	GREEK SMALL LETTER UPSILON WITH DASIA AND OXIA
					break;
				case 0x1F5F:		//	Ὗ	0xe1 0xbd 0x9f	GREEK CAPITAL LETTER UPSILON WITH DASIA AND PERISPOMENI
					*p = 0x1F57;	//	ὗ	0xe1 0xbd 0x97	GREEK SMALL LETTER UPSILON WITH DASIA AND PERISPOMENI
					break;
				case 0x1F68:		//	Ὠ	0xe1 0xbd 0xa8	GREEK CAPITAL LETTER OMEGA WITH PSILI
					*p = 0x1F60;	//	ὠ	0xe1 0xbd 0xa0	GREEK SMALL LETTER OMEGA WITH PSILI
					break;
				case 0x1F69:		//	Ὡ	0xe1 0xbd 0xa9	GREEK CAPITAL LETTER OMEGA WITH DASIA
					*p = 0x1F61;	//	ὡ	0xe1 0xbd 0xa1	GREEK SMALL LETTER OMEGA WITH DASIA
					break;
				case 0x1F6A:		//	Ὢ	0xe1 0xbd 0xaa	GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA
					*p = 0x1F62;	//	ὢ	0xe1 0xbd 0xa2	GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA
					break;
				case 0x1F6B:		//	Ὣ	0xe1 0xbd 0xab	GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA
					*p = 0x1F63;	//	ὣ	0xe1 0xbd 0xa3	GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA
					break;
				case 0x1F6C:		//	Ὤ	0xe1 0xbd 0xac	GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA
					*p = 0x1F64;	//	ὤ	0xe1 0xbd 0xa4	GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA
					break;
				case 0x1F6D:		//	Ὥ	0xe1 0xbd 0xad	GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA
					*p = 0x1F65;	//	ὥ	0xe1 0xbd 0xa5	GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA
					break;
				case 0x1F6E:		//	Ὦ	0xe1 0xbd 0xae	GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI
					*p = 0x1F66;	//	ὦ	0xe1 0xbd 0xa6	GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI
					break;
				case 0x1F6F:		//	Ὧ	0xe1 0xbd 0xaf	GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI
					*p = 0x1F67;	//	ὧ	0xe1 0xbd 0xa7	GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI
					break;
				case 0x1F88:		//	ᾈ	0xe1 0xbe 0x88	GREEK CAPITAL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI
					*p = 0x1F80;	//	ᾀ	0xe1 0xbe 0x80	GREEK SMALL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI
					break;
				case 0x1F89:		//	ᾉ	0xe1 0xbe 0x89	GREEK CAPITAL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI
					*p = 0x1F81;	//	ᾁ	0xe1 0xbe 0x81	GREEK SMALL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI
					break;
				case 0x1F8A:		//	ᾊ	0xe1 0xbe 0x8a	GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F82;	//	ᾂ	0xe1 0xbe 0x82	GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F8B:		//	ᾋ	0xe1 0xbe 0x8b	GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F83;	//	ᾃ	0xe1 0xbe 0x83	GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F8C:		//	ᾌ	0xe1 0xbe 0x8c	GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F84;	//	ᾄ	0xe1 0xbe 0x84	GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F8D:		//	ᾍ	0xe1 0xbe 0x8d	GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F85;	//	ᾅ	0xe1 0xbe 0x85	GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F8E:		//	ᾎ	0xe1 0xbe 0x8e	GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F86;	//	ᾆ	0xe1 0xbe 0x86	GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1F8F:		//	ᾏ	0xe1 0xbe 0x8f	GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F87;	//	ᾇ	0xe1 0xbe 0x87	GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1F98:		//	ᾘ	0xe1 0xbe 0x98	GREEK CAPITAL LETTER ETA WITH PSILI AND PROSGEGRAMMENI
					*p = 0x1F90;	//	ᾐ	0xe1 0xbe 0x90	GREEK SMALL LETTER ETA WITH PSILI AND PROSGEGRAMMENI
					break;
				case 0x1F99:		//	ᾙ	0xe1 0xbe 0x99	GREEK CAPITAL LETTER ETA WITH DASIA AND PROSGEGRAMMENI
					*p = 0x1F91;	//	ᾑ	0xe1 0xbe 0x91	GREEK SMALL LETTER ETA WITH DASIA AND PROSGEGRAMMENI
					break;
				case 0x1F9A:		//	ᾚ	0xe1 0xbe 0x9a	GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F92;	//	ᾒ	0xe1 0xbe 0x92	GREEK SMALL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F9B:		//	ᾛ	0xe1 0xbe 0x9b	GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					*p = 0x1F93;	//	ᾓ	0xe1 0xbe 0x93	GREEK SMALL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1F9C:		//	ᾜ	0xe1 0xbe 0x9c	GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F94;	//	ᾔ	0xe1 0xbe 0x94	GREEK SMALL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F9D:		//	ᾝ	0xe1 0xbe 0x9d	GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					*p = 0x1F95;	//	ᾕ	0xe1 0xbe 0x95	GREEK SMALL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1F9E:		//	ᾞ	0xe1 0xbe 0x9e	GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F96;	//	ᾖ	0xe1 0xbe 0x96	GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1F9F:		//	ᾟ	0xe1 0xbe 0x9f	GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1F97;	//	ᾗ	0xe1 0xbe 0x97	GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1FA8:		//	ᾨ	0xe1 0xbe 0xa8	GREEK CAPITAL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI
					*p = 0x1FA0;	//	ᾠ	0xe1 0xbe 0xa0	GREEK SMALL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI
					break;
				case 0x1FA9:		//	ᾩ	0xe1 0xbe 0xa9	GREEK CAPITAL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI
					*p = 0x1FA1;	//	ᾡ	0xe1 0xbe 0xa1	GREEK SMALL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI
					break;
				case 0x1FAA:		//	ᾪ	0xe1 0xbe 0xaa	GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					*p = 0x1FA2;	//	ᾢ	0xe1 0xbe 0xa2	GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1FAB:		//	ᾫ	0xe1 0xbe 0xab	GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					*p = 0x1FA3;	//	ᾣ	0xe1 0xbe 0xa3	GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI
					break;
				case 0x1FAC:		//	ᾬ	0xe1 0xbe 0xac	GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					*p = 0x1FA4;	//	ᾤ	0xe1 0xbe 0xa4	GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1FAD:		//	ᾭ	0xe1 0xbe 0xad	GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					*p = 0x1FA5;	//	ᾥ	0xe1 0xbe 0xa5	GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI
					break;
				case 0x1FAE:		//	ᾮ	0xe1 0xbe 0xae	GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1FA6;	//	ᾦ	0xe1 0xbe 0xa6	GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1FAF:		//	ᾯ	0xe1 0xbe 0xaf	GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					*p = 0x1FA7;	//	ᾧ	0xe1 0xbe 0xa7	GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI
					break;
				case 0x1FB8:		//	Ᾰ	0xe1 0xbe 0xb8	GREEK CAPITAL LETTER ALPHA WITH VRACHY
					*p = 0x1FB0;	//	ᾰ	0xe1 0xbe 0xb0	GREEK SMALL LETTER ALPHA WITH VRACHY
					break;
				case 0x1FB9:		//	Ᾱ	0xe1 0xbe 0xb9	GREEK CAPITAL LETTER ALPHA WITH MACRON
					*p = 0x1FB1;	//	ᾱ	0xe1 0xbe 0xb1	GREEK SMALL LETTER ALPHA WITH MACRON
					break;
				case 0x1FBA:		//	Ὰ	0xe1 0xbe 0xba	GREEK CAPITAL LETTER ALPHA WITH VARIA
					*p = 0x1F70;	//	ὰ	0xe1 0xbd 0xb0	GREEK SMALL LETTER ALPHA WITH VARIA
					break;
				case 0x1FBB:		//	Ά	0xe1 0xbe 0xbb	GREEK CAPITAL LETTER ALPHA WITH OXIA
					*p = 0x1F71;	//	ά	0xe1 0xbd 0xb1	GREEK SMALL LETTER ALPHA WITH OXIA
					break;
				case 0x1FBC:		//	ᾼ	0xe1 0xbe 0xbc	GREEK CAPITAL LETTER ALPHA WITH PROSGEGRAMMENI
					*p = 0x1FB3;	//	ᾳ	0xe1 0xbe 0xb3	GREEK SMALL LETTER ALPHA WITH PROSGEGRAMMENI
					break;
				case 0x1FC8:		//	Ὲ	0xe1 0xbf 0x88	GREEK CAPITAL LETTER EPSILON WITH VARIA
					*p = 0x1F72;	//	ὲ	0xe1 0xbd 0xb2	GREEK SMALL LETTER EPSILON WITH VARIA
					break;
				case 0x1FC9:		//	Έ	0xe1 0xbf 0x89	GREEK CAPITAL LETTER EPSILON WITH OXIA
					*p = 0x1F73;	//	έ	0xe1 0xbd 0xb3	GREEK SMALL LETTER EPSILON WITH OXIA
					break;
				case 0x1FCA:		//	Ὴ	0xe1 0xbf 0x8a	GREEK CAPITAL LETTER ETA WITH VARIA
					*p = 0x1F74;	//	ὴ	0xe1 0xbd 0xb4	GREEK SMALL LETTER ETA WITH VARIA
					break;
				case 0x1FCB:		//	Ή	0xe1 0xbf 0x8b	GREEK CAPITAL LETTER ETA WITH OXIA
					*p = 0x1F75;	//	ή	0xe1 0xbd 0xb5	GREEK SMALL LETTER ETA WITH OXIA
					break;
				case 0x1FCC:		//	ῌ	0xe1 0xbf 0x8c	GREEK CAPITAL LETTER ETA WITH PROSGEGRAMMENI
					*p = 0x1FC3;	//	ῃ	0xe1 0xbf 0x83	GREEK SMALL LETTER ETA WITH PROSGEGRAMMENI
					break;
				case 0x1FD8:		//	Ῐ	0xe1 0xbf 0x98	GREEK CAPITAL LETTER IOTA WITH VRACHY
					*p = 0x1FD0;	//	ῐ	0xe1 0xbf 0x90	GREEK SMALL LETTER IOTA WITH VRACHY
					break;
				case 0x1FD9:		//	Ῑ	0xe1 0xbf 0x99	GREEK CAPITAL LETTER IOTA WITH MACRON
					*p = 0x1FD1;	//	ῑ	0xe1 0xbf 0x91	GREEK SMALL LETTER IOTA WITH MACRON
					break;
				case 0x1FDA:		//	Ὶ	0xe1 0xbf 0x9a	GREEK CAPITAL LETTER IOTA WITH VARIA
					*p = 0x1F76;	//	ὶ	0xe1 0xbd 0xb6	GREEK SMALL LETTER IOTA WITH VARIA
					break;
				case 0x1FDB:		//	Ί	0xe1 0xbf 0x9b	GREEK CAPITAL LETTER IOTA WITH OXIA
					*p = 0x1F77;	//	ί	0xe1 0xbd 0xb7	GREEK SMALL LETTER IOTA WITH OXIA
					break;
				case 0x1FE8:		//	Ῠ	0xe1 0xbf 0xa8	GREEK CAPITAL LETTER UPSILON WITH VRACHY
					*p = 0x1FE0;	//	ῠ	0xe1 0xbf 0xa0	GREEK SMALL LETTER UPSILON WITH VRACHY
					break;
				case 0x1FE9:		//	Ῡ	0xe1 0xbf 0xa9	GREEK CAPITAL LETTER UPSILON WITH MACRON
					*p = 0x1FE1;	//	ῡ	0xe1 0xbf 0xa1	GREEK SMALL LETTER UPSILON WITH MACRON
					break;
				case 0x1FEA:		//	Ὺ	0xe1 0xbf 0xaa	GREEK CAPITAL LETTER UPSILON WITH VARIA
					*p = 0x1F7A;	//	ὺ	0xe1 0xbd 0xba	GREEK SMALL LETTER UPSILON WITH VARIA
					break;
				case 0x1FEB:		//	Ύ	0xe1 0xbf 0xab	GREEK CAPITAL LETTER UPSILON WITH OXIA
					*p = 0x1F7B;	//	ύ	0xe1 0xbd 0xbb	GREEK SMALL LETTER UPSILON WITH OXIA
					break;
				case 0x1FEC:		//	Ῥ	0xe1 0xbf 0xac	GREEK CAPITAL LETTER RHO WITH DASIA
					*p = 0x1FE5;	//	ῥ	0xe1 0xbf 0xa5	GREEK SMALL LETTER RHO WITH DASIA
					break;
				case 0x1FF8:		//	Ὸ	0xe1 0xbf 0xb8	GREEK CAPITAL LETTER OMICRON WITH VARIA
					*p = 0x1F78;	//	ὸ	0xe1 0xbd 0xb8	GREEK SMALL LETTER OMICRON WITH VARIA
					break;
				case 0x1FF9:		//	Ό	0xe1 0xbf 0xb9	GREEK CAPITAL LETTER OMICRON WITH OXIA
					*p = 0x1F79;	//	ό	0xe1 0xbd 0xb9	GREEK SMALL LETTER OMICRON WITH OXIA
					break;
				case 0x1FFA:		//	Ὼ	0xe1 0xbf 0xba	GREEK CAPITAL LETTER OMEGA WITH VARIA
					*p = 0x1F7C;	//	ὼ	0xe1 0xbd 0xbc	GREEK SMALL LETTER OMEGA WITH VARIA
					break;
				case 0x1FFB:		//	Ώ	0xe1 0xbf 0xbb	GREEK CAPITAL LETTER OMEGA WITH OXIA
					*p = 0x1F7D;	//	ώ	0xe1 0xbd 0xbd	GREEK SMALL LETTER OMEGA WITH OXIA
					break;
				case 0x1FFC:		//	ῼ	0xe1 0xbf 0xbc	GREEK CAPITAL LETTER OMEGA WITH PROSGEGRAMMENI
					*p = 0x1FF3;	//	ῳ	0xe1 0xbf 0xb3	GREEK SMALL LETTER OMEGA WITH PROSGEGRAMMENI
					break;
				case 0x2C00:		//	Ⰰ	0xe2 0xb0 0x80	GLAGOLITIC CAPITAL LETTER AZU
					*p = 0x2C30;	//	ⰰ	0xe2 0xb0 0xb0	GLAGOLITIC SMALL LETTER AZU
					break;
				case 0x2C01:		//	Ⰱ	0xe2 0xb0 0x81	GLAGOLITIC CAPITAL LETTER BUKY
					*p = 0x2C31;	//	ⰱ	0xe2 0xb0 0xb1	GLAGOLITIC SMALL LETTER BUKY
					break;
				case 0x2C02:		//	Ⰲ	0xe2 0xb0 0x82	GLAGOLITIC CAPITAL LETTER VEDE
					*p = 0x2C32;	//	ⰲ	0xe2 0xb0 0xb2	GLAGOLITIC SMALL LETTER VEDE
					break;
				case 0x2C03:		//	Ⰳ	0xe2 0xb0 0x83	GLAGOLITIC CAPITAL LETTER GLAGOLI
					*p = 0x2C33;	//	ⰳ	0xe2 0xb0 0xb3	GLAGOLITIC SMALL LETTER GLAGOLI
					break;
				case 0x2C04:		//	Ⰴ	0xe2 0xb0 0x84	GLAGOLITIC CAPITAL LETTER DOBRO
					*p = 0x2C34;	//	ⰴ	0xe2 0xb0 0xb4	GLAGOLITIC SMALL LETTER DOBRO
					break;
				case 0x2C05:		//	Ⰵ	0xe2 0xb0 0x85	GLAGOLITIC CAPITAL LETTER YESTU
					*p = 0x2C35;	//	ⰵ	0xe2 0xb0 0xb5	GLAGOLITIC SMALL LETTER YESTU
					break;
				case 0x2C06:		//	Ⰶ	0xe2 0xb0 0x86	GLAGOLITIC CAPITAL LETTER ZHIVETE
					*p = 0x2C36;	//	ⰶ	0xe2 0xb0 0xb6	GLAGOLITIC SMALL LETTER ZHIVETE
					break;
				case 0x2C07:		//	Ⰷ	0xe2 0xb0 0x87	GLAGOLITIC CAPITAL LETTER DZELO
					*p = 0x2C37;	//	ⰷ	0xe2 0xb0 0xb7	GLAGOLITIC SMALL LETTER DZELO
					break;
				case 0x2C08:		//	Ⰸ	0xe2 0xb0 0x88	GLAGOLITIC CAPITAL LETTER ZEMLJA
					*p = 0x2C38;	//	ⰸ	0xe2 0xb0 0xb8	GLAGOLITIC SMALL LETTER ZEMLJA
					break;
				case 0x2C09:		//	Ⰹ	0xe2 0xb0 0x89	GLAGOLITIC CAPITAL LETTER IZHE
					*p = 0x2C39;	//	ⰹ	0xe2 0xb0 0xb9	GLAGOLITIC SMALL LETTER IZHE
					break;
				case 0x2C0A:		//	Ⰺ	0xe2 0xb0 0x8a	GLAGOLITIC CAPITAL LETTER INITIAL IZHE
					*p = 0x2C3A;	//	ⰺ	0xe2 0xb0 0xba	GLAGOLITIC SMALL LETTER INITIAL IZHE
					break;
				case 0x2C0B:		//	Ⰻ	0xe2 0xb0 0x8b	GLAGOLITIC CAPITAL LETTER I
					*p = 0x2C3B;	//	ⰻ	0xe2 0xb0 0xbb	GLAGOLITIC SMALL LETTER I
					break;
				case 0x2C0C:		//	Ⰼ	0xe2 0xb0 0x8c	GLAGOLITIC CAPITAL LETTER DJERVI
					*p = 0x2C3C;	//	ⰼ	0xe2 0xb0 0xbc	GLAGOLITIC SMALL LETTER DJERVI
					break;
				case 0x2C0D:		//	Ⰽ	0xe2 0xb0 0x8d	GLAGOLITIC CAPITAL LETTER KAKO
					*p = 0x2C3D;	//	ⰽ	0xe2 0xb0 0xbd	GLAGOLITIC SMALL LETTER KAKO
					break;
				case 0x2C0E:		//	Ⰾ	0xe2 0xb0 0x8e	GLAGOLITIC CAPITAL LETTER LJUDIJE
					*p = 0x2C3E;	//	ⰾ	0xe2 0xb0 0xbe	GLAGOLITIC SMALL LETTER LJUDIJE
					break;
				case 0x2C0F:		//	Ⰿ	0xe2 0xb0 0x8f	GLAGOLITIC CAPITAL LETTER MYSLITE
					*p = 0x2C3F;	//	ⰿ	0xe2 0xb0 0xbf	GLAGOLITIC SMALL LETTER MYSLITE
					break;
				case 0x2C10:		//	Ⱀ	0xe2 0xb0 0x90	GLAGOLITIC CAPITAL LETTER NASHI
					*p = 0x2C40;	//	ⱀ	0xe2 0xb1 0x80	GLAGOLITIC SMALL LETTER NASHI
					break;
				case 0x2C11:		//	Ⱁ	0xe2 0xb0 0x91	GLAGOLITIC CAPITAL LETTER ONU
					*p = 0x2C41;	//	ⱁ	0xe2 0xb1 0x81	GLAGOLITIC SMALL LETTER ONU
					break;
				case 0x2C12:		//	Ⱂ	0xe2 0xb0 0x92	GLAGOLITIC CAPITAL LETTER POKOJI
					*p = 0x2C42;	//	ⱂ	0xe2 0xb1 0x82	GLAGOLITIC SMALL LETTER POKOJI
					break;
				case 0x2C13:		//	Ⱃ	0xe2 0xb0 0x93	GLAGOLITIC CAPITAL LETTER RITSI
					*p = 0x2C43;	//	ⱃ	0xe2 0xb1 0x83	GLAGOLITIC SMALL LETTER RITSI
					break;
				case 0x2C14:		//	Ⱄ	0xe2 0xb0 0x94	GLAGOLITIC CAPITAL LETTER SLOVO
					*p = 0x2C44;	//	ⱄ	0xe2 0xb1 0x84	GLAGOLITIC SMALL LETTER SLOVO
					break;
				case 0x2C15:		//	Ⱅ	0xe2 0xb0 0x95	GLAGOLITIC CAPITAL LETTER TVRIDO
					*p = 0x2C45;	//	ⱅ	0xe2 0xb1 0x85	GLAGOLITIC SMALL LETTER TVRIDO
					break;
				case 0x2C16:		//	Ⱆ	0xe2 0xb0 0x96	GLAGOLITIC CAPITAL LETTER UKU
					*p = 0x2C46;	//	ⱆ	0xe2 0xb1 0x86	GLAGOLITIC SMALL LETTER UKU
					break;
				case 0x2C17:		//	Ⱇ	0xe2 0xb0 0x97	GLAGOLITIC CAPITAL LETTER FRITU
					*p = 0x2C47;	//	ⱇ	0xe2 0xb1 0x87	GLAGOLITIC SMALL LETTER FRITU
					break;
				case 0x2C18:		//	Ⱈ	0xe2 0xb0 0x98	GLAGOLITIC CAPITAL LETTER HERU
					*p = 0x2C48;	//	ⱈ	0xe2 0xb1 0x88	GLAGOLITIC SMALL LETTER HERU
					break;
				case 0x2C19:		//	Ⱉ	0xe2 0xb0 0x99	GLAGOLITIC CAPITAL LETTER OTU
					*p = 0x2C49;	//	ⱉ	0xe2 0xb1 0x89	GLAGOLITIC SMALL LETTER OTU
					break;
				case 0x2C1A:		//	Ⱊ	0xe2 0xb0 0x9a	GLAGOLITIC CAPITAL LETTER PE
					*p = 0x2C4A;	//	ⱊ	0xe2 0xb1 0x8a	GLAGOLITIC SMALL LETTER PE
					break;
				case 0x2C1B:		//	Ⱋ	0xe2 0xb0 0x9b	GLAGOLITIC CAPITAL LETTER SHTA
					*p = 0x2C4B;	//	ⱋ	0xe2 0xb1 0x8b	GLAGOLITIC SMALL LETTER SHTA
					break;
				case 0x2C1C:		//	Ⱌ	0xe2 0xb0 0x9c	GLAGOLITIC CAPITAL LETTER TSI
					*p = 0x2C4C;	//	ⱌ	0xe2 0xb1 0x8c	GLAGOLITIC SMALL LETTER TSI
					break;
				case 0x2C1D:		//	Ⱍ	0xe2 0xb0 0x9d	GLAGOLITIC CAPITAL LETTER CHRIVI
					*p = 0x2C4D;	//	ⱍ	0xe2 0xb1 0x8d	GLAGOLITIC SMALL LETTER CHRIVI
					break;
				case 0x2C1E:		//	Ⱎ	0xe2 0xb0 0x9e	GLAGOLITIC CAPITAL LETTER SHA
					*p = 0x2C4E;	//	ⱎ	0xe2 0xb1 0x8e	GLAGOLITIC SMALL LETTER SHA
					break;
				case 0x2C1F:		//	Ⱏ	0xe2 0xb0 0x9f	GLAGOLITIC CAPITAL LETTER YERU
					*p = 0x2C4F;	//	ⱏ	0xe2 0xb1 0x8f	GLAGOLITIC SMALL LETTER YERU
					break;
				case 0x2C20:		//	Ⱐ	0xe2 0xb0 0xa0	GLAGOLITIC CAPITAL LETTER YERI
					*p = 0x2C50;	//	ⱐ	0xe2 0xb1 0x90	GLAGOLITIC SMALL LETTER YERI
					break;
				case 0x2C21:		//	Ⱑ	0xe2 0xb0 0xa1	GLAGOLITIC CAPITAL LETTER YATI
					*p = 0x2C51;	//	ⱑ	0xe2 0xb1 0x91	GLAGOLITIC SMALL LETTER YATI
					break;
				case 0x2C22:		//	Ⱒ	0xe2 0xb0 0xa2	GLAGOLITIC CAPITAL LETTER SPIDERY HA
					*p = 0x2C52;	//	ⱒ	0xe2 0xb1 0x92	GLAGOLITIC SMALL LETTER SPIDERY HA
					break;
				case 0x2C23:		//	Ⱓ	0xe2 0xb0 0xa3	GLAGOLITIC CAPITAL LETTER YU
					*p = 0x2C53;	//	ⱓ	0xe2 0xb1 0x93	GLAGOLITIC SMALL LETTER YU
					break;
				case 0x2C24:		//	Ⱔ	0xe2 0xb0 0xa4	GLAGOLITIC CAPITAL LETTER SMALL YUS
					*p = 0x2C54;	//	ⱔ	0xe2 0xb1 0x94	GLAGOLITIC SMALL LETTER SMALL YUS
					break;
				case 0x2C25:		//	Ⱕ	0xe2 0xb0 0xa5	GLAGOLITIC CAPITAL LETTER SMALL YUS WITH TAIL
					*p = 0x2C55;	//	ⱕ	0xe2 0xb1 0x95	GLAGOLITIC SMALL LETTER SMALL YUS WITH TAIL
					break;
				case 0x2C26:		//	Ⱖ	0xe2 0xb0 0xa6	GLAGOLITIC CAPITAL LETTER YO
					*p = 0x2C56;	//	ⱖ	0xe2 0xb1 0x96	GLAGOLITIC SMALL LETTER YO
					break;
				case 0x2C27:		//	Ⱗ	0xe2 0xb0 0xa7	GLAGOLITIC CAPITAL LETTER IOTATED SMALL YUS
					*p = 0x2C57;	//	ⱗ	0xe2 0xb1 0x97	GLAGOLITIC SMALL LETTER IOTATED SMALL YUS
					break;
				case 0x2C28:		//	Ⱘ	0xe2 0xb0 0xa8	GLAGOLITIC CAPITAL LETTER BIG YUS
					*p = 0x2C58;	//	ⱘ	0xe2 0xb1 0x98	GLAGOLITIC SMALL LETTER BIG YUS
					break;
				case 0x2C29:		//	Ⱙ	0xe2 0xb0 0xa9	GLAGOLITIC CAPITAL LETTER IOTATED BIG YUS
					*p = 0x2C59;	//	ⱙ	0xe2 0xb1 0x99	GLAGOLITIC SMALL LETTER IOTATED BIG YUS
					break;
				case 0x2C2A:		//	Ⱚ	0xe2 0xb0 0xaa	GLAGOLITIC CAPITAL LETTER FITA
					*p = 0x2C5A;	//	ⱚ	0xe2 0xb1 0x9a	GLAGOLITIC SMALL LETTER FITA
					break;
				case 0x2C2B:		//	Ⱛ	0xe2 0xb0 0xab	GLAGOLITIC CAPITAL LETTER IZHITSA
					*p = 0x2C5B;	//	ⱛ	0xe2 0xb1 0x9b	GLAGOLITIC SMALL LETTER IZHITSA
					break;
				case 0x2C2C:		//	Ⱜ	0xe2 0xb0 0xac	GLAGOLITIC CAPITAL LETTER SHTAPIC
					*p = 0x2C5C;	//	ⱜ	0xe2 0xb1 0x9c	GLAGOLITIC SMALL LETTER SHTAPIC
					break;
				case 0x2C2D:		//	Ⱝ	0xe2 0xb0 0xad	GLAGOLITIC CAPITAL LETTER TROKUTASTI A
					*p = 0x2C5D;	//	ⱝ	0xe2 0xb1 0x9d	GLAGOLITIC SMALL LETTER TROKUTASTI A
					break;
				case 0x2C2E:		//	Ⱞ	0xe2 0xb0 0xae	GLAGOLITIC CAPITAL LETTER LATINATE MYSLITE
					*p = 0x2C5E;	//	ⱞ	0xe2 0xb1 0x9e	GLAGOLITIC SMALL LETTER LATINATE MYSLITE
					break;
				case 0x2C60:		//	Ⱡ	0xe2 0xb1 0xa0	LATIN CAPITAL LETTER L WITH DOUBLE BAR
					*p = 0x2C61;	//	ⱡ	0xe2 0xb1 0xa1	LATIN SMALL LETTER L WITH DOUBLE BAR
					break;
				case 0x2C62:		//	Ɫ	0xe2 0xb1 0xa2	LATIN CAPITAL LETTER L WITH MIDDLE TILDE
					*p = 0x026B;	//	ɫ	0xc9 0xab	LATIN SMALL LETTER L WITH MIDDLE TILDE
					break;
				case 0x2C63:		//	Ᵽ	0xe2 0xb1 0xa3	LATIN CAPITAL LETTER P WITH STROKE
					*p = 0x1D7D;	//	ᵽ	0xe1 0xb5 0xbd	LATIN SMALL LETTER P WITH STROKE
					break;
				case 0x2C64:		//	Ɽ	0xe2 0xb1 0xa4	LATIN CAPITAL LETTER R WITH TAIL
					*p = 0x027D;	//	ɽ	0xc9 0xbd	LATIN SMALL LETTER R WITH TAIL
					break;
				case 0x2C67:		//	Ⱨ	0xe2 0xb1 0xa7	LATIN CAPITAL LETTER H WITH DESCENDER
					*p = 0x2C68;	//	ⱨ	0xe2 0xb1 0xa8	LATIN SMALL LETTER H WITH DESCENDER
					break;
				case 0x2C69:		//	Ⱪ	0xe2 0xb1 0xa9	LATIN CAPITAL LETTER K WITH DESCENDER
					*p = 0x2C6A;	//	ⱪ	0xe2 0xb1 0xaa	LATIN SMALL LETTER K WITH DESCENDER
					break;
				case 0x2C6B:		//	Ⱬ	0xe2 0xb1 0xab	LATIN CAPITAL LETTER Z WITH DESCENDER
					*p = 0x2C6C;	//	ⱬ	0xe2 0xb1 0xac	LATIN SMALL LETTER Z WITH DESCENDER
					break;
				case 0x2C6D:		//	Ɑ	0xe2 0xb1 0xad	LATIN CAPITAL LETTER ALPHA
					*p = 0x0251;	//	ɑ	0xc9 0x91	LATIN SMALL LETTER ALPHA
					break;
				case 0x2C6E:		//	Ɱ	0xe2 0xb1 0xae	LATIN CAPITAL LETTER M WITH HOOK
					*p = 0x0271;	//	ɱ	0xc9 0xb1	LATIN SMALL LETTER M WITH HOOK
					break;
				case 0x2C6F:		//	Ɐ	0xe2 0xb1 0xaf	LATIN CAPITAL LETTER TURNED A
					*p = 0x0250;	//	ɐ	0xc9 0x90	LATIN SMALL LETTER TURNED A
					break;
				case 0x2C70:		//	Ɒ	0xe2 0xb1 0xb0	LATIN CAPITAL LETTER TURNED ALPHA
					*p = 0x0252;	//	ɒ	0xc9 0x92	LATIN SMALL LETTER TURNED ALPHA
					break;
				case 0x2C72:		//	Ⱳ	0xe2 0xb1 0xb2	LATIN CAPITAL LETTER W WITH HOOK
					*p = 0x2C73;	//	ⱳ	0xe2 0xb1 0xb3	LATIN SMALL LETTER W WITH HOOK
					break;
				case 0x2C75:		//	Ⱶ	0xe2 0xb1 0xb5	LATIN CAPITAL LETTER HALF H
					*p = 0x2C76;	//	ⱶ	0xe2 0xb1 0xb6	LATIN SMALL LETTER HALF H
					break;
				case 0x2C7E:		//	Ȿ	0xe2 0xb1 0xbe	LATIN CAPITAL LETTER S WITH SWASH TAIL
					*p = 0x023F;	//	ȿ	0xc8 0xbf	LATIN SMALL LETTER S WITH SWASH TAIL
					break;
				case 0x2C7F:		//	Ɀ	0xe2 0xb1 0xbf	LATIN CAPITAL LETTER Z WITH SWASH TAIL
					*p = 0x0240;	//	ɀ	0xc9 0x80	LATIN SMALL LETTER Z WITH SWASH TAIL
					break;
				case 0x2C80:		//	Ⲁ	0xe2 0xb2 0x80	COPTIC CAPITAL LETTER ALFA
					*p = 0x2C81;	//	ⲁ	0xe2 0xb2 0x81	COPTIC SMALL LETTER ALFA
					break;
				case 0x2C82:		//	Ⲃ	0xe2 0xb2 0x82	COPTIC CAPITAL LETTER VIDA
					*p = 0x2C83;	//	ⲃ	0xe2 0xb2 0x83	COPTIC SMALL LETTER VIDA
					break;
				case 0x2C84:		//	Ⲅ	0xe2 0xb2 0x84	COPTIC CAPITAL LETTER GAMMA
					*p = 0x2C85;	//	ⲅ	0xe2 0xb2 0x85	COPTIC SMALL LETTER GAMMA
					break;
				case 0x2C86:		//	Ⲇ	0xe2 0xb2 0x86	COPTIC CAPITAL LETTER DALDA
					*p = 0x2C87;	//	ⲇ	0xe2 0xb2 0x87	COPTIC SMALL LETTER DALDA
					break;
				case 0x2C88:		//	Ⲉ	0xe2 0xb2 0x88	COPTIC CAPITAL LETTER EIE
					*p = 0x2C89;	//	ⲉ	0xe2 0xb2 0x89	COPTIC SMALL LETTER EIE
					break;
				case 0x2C8A:		//	Ⲋ	0xe2 0xb2 0x8a	COPTIC CAPITAL LETTER SOU
					*p = 0x2C8B;	//	ⲋ	0xe2 0xb2 0x8b	COPTIC SMALL LETTER SOU
					break;
				case 0x2C8C:		//	Ⲍ	0xe2 0xb2 0x8c	COPTIC CAPITAL LETTER ZATA
					*p = 0x2C8D;	//	ⲍ	0xe2 0xb2 0x8d	COPTIC SMALL LETTER ZATA
					break;
				case 0x2C8E:		//	Ⲏ	0xe2 0xb2 0x8e	COPTIC CAPITAL LETTER HATE
					*p = 0x2C8F;	//	ⲏ	0xe2 0xb2 0x8f	COPTIC SMALL LETTER HATE
					break;
				case 0x2C90:		//	Ⲑ	0xe2 0xb2 0x90	COPTIC CAPITAL LETTER THETHE
					*p = 0x2C91;	//	ⲑ	0xe2 0xb2 0x91	COPTIC SMALL LETTER THETHE
					break;
				case 0x2C92:		//	Ⲓ	0xe2 0xb2 0x92	COPTIC CAPITAL LETTER IAUDA
					*p = 0x2C93;	//	ⲓ	0xe2 0xb2 0x93	COPTIC SMALL LETTER IAUDA
					break;
				case 0x2C94:		//	Ⲕ	0xe2 0xb2 0x94	COPTIC CAPITAL LETTER KAPA
					*p = 0x2C95;	//	ⲕ	0xe2 0xb2 0x95	COPTIC SMALL LETTER KAPA
					break;
				case 0x2C96:		//	Ⲗ	0xe2 0xb2 0x96	COPTIC CAPITAL LETTER LAULA
					*p = 0x2C97;	//	ⲗ	0xe2 0xb2 0x97	COPTIC SMALL LETTER LAULA
					break;
				case 0x2C98:		//	Ⲙ	0xe2 0xb2 0x98	COPTIC CAPITAL LETTER MI
					*p = 0x2C99;	//	ⲙ	0xe2 0xb2 0x99	COPTIC SMALL LETTER MI
					break;
				case 0x2C9A:		//	Ⲛ	0xe2 0xb2 0x9a	COPTIC CAPITAL LETTER NI
					*p = 0x2C9B;	//	ⲛ	0xe2 0xb2 0x9b	COPTIC SMALL LETTER NI
					break;
				case 0x2C9C:		//	Ⲝ	0xe2 0xb2 0x9c	COPTIC CAPITAL LETTER KSI
					*p = 0x2C9D;	//	ⲝ	0xe2 0xb2 0x9d	COPTIC SMALL LETTER KSI
					break;
				case 0x2C9E:		//	Ⲟ	0xe2 0xb2 0x9e	COPTIC CAPITAL LETTER O
					*p = 0x2C9F;	//	ⲟ	0xe2 0xb2 0x9f	COPTIC SMALL LETTER O
					break;
				case 0x2CA0:		//	Ⲡ	0xe2 0xb2 0xa0	COPTIC CAPITAL LETTER PI
					*p = 0x2CA1;	//	ⲡ	0xe2 0xb2 0xa1	COPTIC SMALL LETTER PI
					break;
				case 0x2CA2:		//	Ⲣ	0xe2 0xb2 0xa2	COPTIC CAPITAL LETTER RO
					*p = 0x2CA3;	//	ⲣ	0xe2 0xb2 0xa3	COPTIC SMALL LETTER RO
					break;
				case 0x2CA4:		//	Ⲥ	0xe2 0xb2 0xa4	COPTIC CAPITAL LETTER SIMA
					*p = 0x2CA5;	//	ⲥ	0xe2 0xb2 0xa5	COPTIC SMALL LETTER SIMA
					break;
				case 0x2CA6:		//	Ⲧ	0xe2 0xb2 0xa6	COPTIC CAPITAL LETTER TAU
					*p = 0x2CA7;	//	ⲧ	0xe2 0xb2 0xa7	COPTIC SMALL LETTER TAU
					break;
				case 0x2CA8:		//	Ⲩ	0xe2 0xb2 0xa8	COPTIC CAPITAL LETTER UA
					*p = 0x2CA9;	//	ⲩ	0xe2 0xb2 0xa9	COPTIC SMALL LETTER UA
					break;
				case 0x2CAA:		//	Ⲫ	0xe2 0xb2 0xaa	COPTIC CAPITAL LETTER FI
					*p = 0x2CAB;	//	ⲫ	0xe2 0xb2 0xab	COPTIC SMALL LETTER FI
					break;
				case 0x2CAC:		//	Ⲭ	0xe2 0xb2 0xac	COPTIC CAPITAL LETTER KHI
					*p = 0x2CAD;	//	ⲭ	0xe2 0xb2 0xad	COPTIC SMALL LETTER KHI
					break;
				case 0x2CAE:		//	Ⲯ	0xe2 0xb2 0xae	COPTIC CAPITAL LETTER PSI
					*p = 0x2CAF;	//	ⲯ	0xe2 0xb2 0xaf	COPTIC SMALL LETTER PSI
					break;
				case 0x2CB0:		//	Ⲱ	0xe2 0xb2 0xb0	COPTIC CAPITAL LETTER OOU
					*p = 0x2CB1;	//	ⲱ	0xe2 0xb2 0xb1	COPTIC SMALL LETTER OOU
					break;
				case 0x2CB2:		//	Ⲳ	0xe2 0xb2 0xb2	COPTIC CAPITAL LETTER DIALECT-P ALEF
					*p = 0x2CB3;	//	ⲳ	0xe2 0xb2 0xb3	COPTIC SMALL LETTER DIALECT-P ALEF
					break;
				case 0x2CB4:		//	Ⲵ	0xe2 0xb2 0xb4	COPTIC CAPITAL LETTER OLD COPTIC AIN
					*p = 0x2CB5;	//	ⲵ	0xe2 0xb2 0xb5	COPTIC SMALL LETTER OLD COPTIC AIN
					break;
				case 0x2CB6:		//	Ⲷ	0xe2 0xb2 0xb6	COPTIC CAPITAL LETTER CRYPTOGRAMMIC EIE
					*p = 0x2CB7;	//	ⲷ	0xe2 0xb2 0xb7	COPTIC SMALL LETTER CRYPTOGRAMMIC EIE
					break;
				case 0x2CB8:		//	Ⲹ	0xe2 0xb2 0xb8	COPTIC CAPITAL LETTER DIALECT-P KAPA
					*p = 0x2CB9;	//	ⲹ	0xe2 0xb2 0xb9	COPTIC SMALL LETTER DIALECT-P KAPA
					break;
				case 0x2CBA:		//	Ⲻ	0xe2 0xb2 0xba	COPTIC CAPITAL LETTER DIALECT-P NI
					*p = 0x2CBB;	//	ⲻ	0xe2 0xb2 0xbb	COPTIC SMALL LETTER DIALECT-P NI
					break;
				case 0x2CBC:		//	Ⲽ	0xe2 0xb2 0xbc	COPTIC CAPITAL LETTER CRYPTOGRAMMIC NI
					*p = 0x2CBD;	//	ⲽ	0xe2 0xb2 0xbd	COPTIC SMALL LETTER CRYPTOGRAMMIC NI
					break;
				case 0x2CBE:		//	Ⲿ	0xe2 0xb2 0xbe	COPTIC CAPITAL LETTER OLD COPTIC OOU
					*p = 0x2CBF;	//	ⲿ	0xe2 0xb2 0xbf	COPTIC SMALL LETTER OLD COPTIC OOU
					break;
				case 0x2CC0:		//	Ⳁ	0xe2 0xb3 0x80	COPTIC CAPITAL LETTER SAMPI
					*p = 0x2CC1;	//	ⳁ	0xe2 0xb3 0x81	COPTIC SMALL LETTER SAMPI
					break;
				case 0x2CC2:		//	Ⳃ	0xe2 0xb3 0x82	COPTIC CAPITAL LETTER CROSSED SHEI
					*p = 0x2CC3;	//	ⳃ	0xe2 0xb3 0x83	COPTIC SMALL LETTER CROSSED SHEI
					break;
				case 0x2CC4:		//	Ⳅ	0xe2 0xb3 0x84	COPTIC CAPITAL LETTER OLD COPTIC SHEI
					*p = 0x2CC5;	//	ⳅ	0xe2 0xb3 0x85	COPTIC SMALL LETTER OLD COPTIC SHEI
					break;
				case 0x2CC6:		//	Ⳇ	0xe2 0xb3 0x86	COPTIC CAPITAL LETTER OLD COPTIC ESH
					*p = 0x2CC7;	//	ⳇ	0xe2 0xb3 0x87	COPTIC SMALL LETTER OLD COPTIC ESH
					break;
				case 0x2CC8:		//	Ⳉ	0xe2 0xb3 0x88	COPTIC CAPITAL LETTER AKHMIMIC KHEI
					*p = 0x2CC9;	//	ⳉ	0xe2 0xb3 0x89	COPTIC SMALL LETTER AKHMIMIC KHEI
					break;
				case 0x2CCA:		//	Ⳋ	0xe2 0xb3 0x8a	COPTIC CAPITAL LETTER DIALECT-P HORI
					*p = 0x2CCB;	//	ⳋ	0xe2 0xb3 0x8b	COPTIC SMALL LETTER DIALECT-P HORI
					break;
				case 0x2CCC:		//	Ⳍ	0xe2 0xb3 0x8c	COPTIC CAPITAL LETTER OLD COPTIC HORI
					*p = 0x2CCD;	//	ⳍ	0xe2 0xb3 0x8d	COPTIC SMALL LETTER OLD COPTIC HORI
					break;
				case 0x2CCE:		//	Ⳏ	0xe2 0xb3 0x8e	COPTIC CAPITAL LETTER OLD COPTIC HA
					*p = 0x2CCF;	//	ⳏ	0xe2 0xb3 0x8f	COPTIC SMALL LETTER OLD COPTIC HA
					break;
				case 0x2CD0:		//	Ⳑ	0xe2 0xb3 0x90	COPTIC CAPITAL LETTER L-SHAPED HA
					*p = 0x2CD1;	//	ⳑ	0xe2 0xb3 0x91	COPTIC SMALL LETTER L-SHAPED HA
					break;
				case 0x2CD2:		//	Ⳓ	0xe2 0xb3 0x92	COPTIC CAPITAL LETTER OLD COPTIC HEI
					*p = 0x2CD3;	//	ⳓ	0xe2 0xb3 0x93	COPTIC SMALL LETTER OLD COPTIC HEI
					break;
				case 0x2CD4:		//	Ⳕ	0xe2 0xb3 0x94	COPTIC CAPITAL LETTER OLD COPTIC HAT
					*p = 0x2CD5;	//	ⳕ	0xe2 0xb3 0x95	COPTIC SMALL LETTER OLD COPTIC HAT
					break;
				case 0x2CD6:		//	Ⳗ	0xe2 0xb3 0x96	COPTIC CAPITAL LETTER OLD COPTIC GANGIA
					*p = 0x2CD7;	//	ⳗ	0xe2 0xb3 0x97	COPTIC SMALL LETTER OLD COPTIC GANGIA
					break;
				case 0x2CD8:		//	Ⳙ	0xe2 0xb3 0x98	COPTIC CAPITAL LETTER OLD COPTIC DJA
					*p = 0x2CD9;	//	ⳙ	0xe2 0xb3 0x99	COPTIC SMALL LETTER OLD COPTIC DJA
					break;
				case 0x2CDA:		//	Ⳛ	0xe2 0xb3 0x9a	COPTIC CAPITAL LETTER OLD COPTIC SHIMA
					*p = 0x2CDB;	//	ⳛ	0xe2 0xb3 0x9b	COPTIC SMALL LETTER OLD COPTIC SHIMA
					break;
				case 0x2CDC:		//	Ⳝ	0xe2 0xb3 0x9c	COPTIC CAPITAL LETTER OLD NUBIAN SHIMA
					*p = 0x2CDD;	//	ⳝ	0xe2 0xb3 0x9d	COPTIC SMALL LETTER OLD NUBIAN SHIMA
					break;
				case 0x2CDE:		//	Ⳟ	0xe2 0xb3 0x9e	COPTIC CAPITAL LETTER OLD NUBIAN NGI
					*p = 0x2CDF;	//	ⳟ	0xe2 0xb3 0x9f	COPTIC SMALL LETTER OLD NUBIAN NGI
					break;
				case 0x2CE0:		//	Ⳡ	0xe2 0xb3 0xa0	COPTIC CAPITAL LETTER OLD NUBIAN NYI
					*p = 0x2CE1;	//	ⳡ	0xe2 0xb3 0xa1	COPTIC SMALL LETTER OLD NUBIAN NYI
					break;
				case 0x2CE2:		//	Ⳣ	0xe2 0xb3 0xa2	COPTIC CAPITAL LETTER OLD NUBIAN WAU
					*p = 0x2CE3;	//	ⳣ	0xe2 0xb3 0xa3	COPTIC SMALL LETTER OLD NUBIAN WAU
					break;
				case 0x2CEB:		//	Ⳬ	0xe2 0xb3 0xab	COPTIC CAPITAL LETTER CRYPTOGRAMMIC SHEI
					*p = 0x2CEC;	//	ⳬ	0xe2 0xb3 0xac	COPTIC SMALL LETTER CRYPTOGRAMMIC SHEI
					break;
				case 0x2CED:		//	Ⳮ	0xe2 0xb3 0xad	COPTIC CAPITAL LETTER CRYPTOGRAMMIC GANGIA
					*p = 0x2CEE;	//	ⳮ	0xe2 0xb3 0xae	COPTIC SMALL LETTER CRYPTOGRAMMIC GANGIA
					break;
				case 0x2CF2:		//	Ⳳ	0xe2 0xb3 0xb2	COPTIC CAPITAL LETTER BOHAIRIC KHEI
					*p = 0x2CF3;	//	ⳳ	0xe2 0xb3 0xb3	COPTIC SMALL LETTER BOHAIRIC KHEI
					break;
				case 0x2D00:		//	ⴀ	0xe2 0xb4 0x80	GEORGIAN LETTER AN
					*p = 0x10D0;	//	ა	0xe1 0x83 0x90	GEORGIAN SMALL LETTER AN
					break;
				case 0x2D01:		//	ⴁ	0xe2 0xb4 0x81	GEORGIAN LETTER BAN
					*p = 0x10D1;	//	ბ	0xe1 0x83 0x91	GEORGIAN SMALL LETTER BAN
					break;
				case 0x2D02:		//	ⴂ	0xe2 0xb4 0x82	GEORGIAN LETTER GAN
					*p = 0x10D2;	//	გ	0xe1 0x83 0x92	GEORGIAN SMALL LETTER GAN
					break;
				case 0x2D03:		//	ⴃ	0xe2 0xb4 0x83	GEORGIAN LETTER DON
					*p = 0x10D3;	//	დ	0xe1 0x83 0x93	GEORGIAN SMALL LETTER DON
					break;
				case 0x2D04:		//	ⴄ	0xe2 0xb4 0x84	GEORGIAN LETTER EN
					*p = 0x10D4;	//	ე	0xe1 0x83 0x94	GEORGIAN SMALL LETTER EN
					break;
				case 0x2D05:		//	ⴅ	0xe2 0xb4 0x85	GEORGIAN LETTER VIN
					*p = 0x10D5;	//	ვ	0xe1 0x83 0x95	GEORGIAN SMALL LETTER VIN
					break;
				case 0x2D06:		//	ⴆ	0xe2 0xb4 0x86	GEORGIAN LETTER ZEN
					*p = 0x10D6;	//	ზ	0xe1 0x83 0x96	GEORGIAN SMALL LETTER ZEN
					break;
				case 0x2D07:		//	ⴇ	0xe2 0xb4 0x87	GEORGIAN LETTER TAN
					*p = 0x10D7;	//	თ	0xe1 0x83 0x97	GEORGIAN SMALL LETTER TAN
					break;
				case 0x2D08:		//	ⴈ	0xe2 0xb4 0x88	GEORGIAN LETTER IN
					*p = 0x10D8;	//	ი	0xe1 0x83 0x98	GEORGIAN SMALL LETTER IN
					break;
				case 0x2D09:		//	ⴉ	0xe2 0xb4 0x89	GEORGIAN LETTER KAN
					*p = 0x10D9;	//	კ	0xe1 0x83 0x99	GEORGIAN SMALL LETTER KAN
					break;
				case 0x2D0A:		//	ⴊ	0xe2 0xb4 0x8a	GEORGIAN LETTER LAS
					*p = 0x10DA;	//	ლ	0xe1 0x83 0x9a	GEORGIAN SMALL LETTER LAS
					break;
				case 0x2D0B:		//	ⴋ	0xe2 0xb4 0x8b	GEORGIAN LETTER MAN
					*p = 0x10DB;	//	მ	0xe1 0x83 0x9b	GEORGIAN SMALL LETTER MAN
					break;
				case 0x2D0C:		//	ⴌ	0xe2 0xb4 0x8c	GEORGIAN LETTER NAR
					*p = 0x10DC;	//	ნ	0xe1 0x83 0x9c	GEORGIAN SMALL LETTER NAR
					break;
				case 0x2D0D:		//	ⴍ	0xe2 0xb4 0x8d	GEORGIAN LETTER ON
					*p = 0x10DD;	//	ო	0xe1 0x83 0x9d	GEORGIAN SMALL LETTER ON
					break;
				case 0x2D0E:		//	ⴎ	0xe2 0xb4 0x8e	GEORGIAN LETTER PAR
					*p = 0x10DE;	//	პ	0xe1 0x83 0x9e	GEORGIAN SMALL LETTER PAR
					break;
				case 0x2D0F:		//	ⴏ	0xe2 0xb4 0x8f	GEORGIAN LETTER ZHAR
					*p = 0x10DF;	//	ჟ	0xe1 0x83 0x9f	GEORGIAN SMALL LETTER ZHAR
					break;
				case 0x2D10:		//	ⴐ	0xe2 0xb4 0x90	GEORGIAN LETTER RAE
					*p = 0x10E0;	//	რ	0xe1 0x83 0xa0	GEORGIAN SMALL LETTER RAE
					break;
				case 0x2D11:		//	ⴑ	0xe2 0xb4 0x91	GEORGIAN LETTER SAN
					*p = 0x10E1;	//	ს	0xe1 0x83 0xa1	GEORGIAN SMALL LETTER SAN
					break;
				case 0x2D12:		//	ⴒ	0xe2 0xb4 0x92	GEORGIAN LETTER TAR
					*p = 0x10E2;	//	ტ	0xe1 0x83 0xa2	GEORGIAN SMALL LETTER TAR
					break;
				case 0x2D13:		//	ⴓ	0xe2 0xb4 0x93	GEORGIAN LETTER UN
					*p = 0x10E3;	//	უ	0xe1 0x83 0xa3	GEORGIAN SMALL LETTER UN
					break;
				case 0x2D14:		//	ⴔ	0xe2 0xb4 0x94	GEORGIAN LETTER PHAR
					*p = 0x10E4;	//	ფ	0xe1 0x83 0xa4	GEORGIAN SMALL LETTER PHAR
					break;
				case 0x2D15:		//	ⴕ	0xe2 0xb4 0x95	GEORGIAN LETTER KHAR
					*p = 0x10E5;	//	ქ	0xe1 0x83 0xa5	GEORGIAN SMALL LETTER KHAR
					break;
				case 0x2D16:		//	ⴖ	0xe2 0xb4 0x96	GEORGIAN LETTER GHAN
					*p = 0x10E6;	//	ღ	0xe1 0x83 0xa6	GEORGIAN SMALL LETTER GHAN
					break;
				case 0x2D17:		//	ⴗ	0xe2 0xb4 0x97	GEORGIAN LETTER QAR
					*p = 0x10E7;	//	ყ	0xe1 0x83 0xa7	GEORGIAN SMALL LETTER QAR
					break;
				case 0x2D18:		//	ⴘ	0xe2 0xb4 0x98	GEORGIAN LETTER SHIN
					*p = 0x10E8;	//	შ	0xe1 0x83 0xa8	GEORGIAN SMALL LETTER SHIN
					break;
				case 0x2D19:		//	ⴙ	0xe2 0xb4 0x99	GEORGIAN LETTER CHIN
					*p = 0x10E9;	//	ჩ	0xe1 0x83 0xa9	GEORGIAN SMALL LETTER CHIN
					break;
				case 0x2D1A:		//	ⴚ	0xe2 0xb4 0x9a	GEORGIAN LETTER CAN
					*p = 0x10EA;	//	ც	0xe1 0x83 0xaa	GEORGIAN SMALL LETTER CAN
					break;
				case 0x2D1B:		//	ⴛ	0xe2 0xb4 0x9b	GEORGIAN LETTER JIL
					*p = 0x10EB;	//	ძ	0xe1 0x83 0xab	GEORGIAN SMALL LETTER JIL
					break;
				case 0x2D1C:		//	ⴜ	0xe2 0xb4 0x9c	GEORGIAN LETTER CIL
					*p = 0x10EC;	//	წ	0xe1 0x83 0xac	GEORGIAN SMALL LETTER CIL
					break;
				case 0x2D1D:		//	ⴝ	0xe2 0xb4 0x9d	GEORGIAN LETTER CHAR
					*p = 0x10ED;	//	ჭ	0xe1 0x83 0xad	GEORGIAN SMALL LETTER CHAR
					break;
				case 0x2D1E:		//	ⴞ	0xe2 0xb4 0x9e	GEORGIAN LETTER XAN
					*p = 0x10EE;	//	ხ	0xe1 0x83 0xae	GEORGIAN SMALL LETTER XAN
					break;
				case 0x2D1F:		//	ⴟ	0xe2 0xb4 0x9f	GEORGIAN LETTER JHAN
					*p = 0x10EF;	//	ჯ	0xe1 0x83 0xaf	GEORGIAN SMALL LETTER JHAN
					break;
				case 0x2D20:		//	ⴠ	0xe2 0xb4 0xa0	GEORGIAN LETTER HAE
					*p = 0x10F0;	//	ჰ	0xe1 0x83 0xb0	GEORGIAN SMALL LETTER HAE
					break;
				case 0x2D21:		//	ⴡ	0xe2 0xb4 0xa1	GEORGIAN LETTER HE
					*p = 0x10F1;	//	ჱ	0xe1 0x83 0xb1	GEORGIAN SMALL LETTER HE
					break;
				case 0x2D22:		//	ⴢ	0xe2 0xb4 0xa2	GEORGIAN LETTER HIE
					*p = 0x10F2;	//	ჲ	0xe1 0x83 0xb2	GEORGIAN SMALL LETTER HIE
					break;
				case 0x2D23:		//	ⴣ	0xe2 0xb4 0xa3	GEORGIAN LETTER WE
					*p = 0x10F3;	//	ჳ	0xe1 0x83 0xb3	GEORGIAN SMALL LETTER WE
					break;
				case 0x2D24:		//	ⴤ	0xe2 0xb4 0xa4	GEORGIAN LETTER HAR
					*p = 0x10F4;	//	ჴ	0xe1 0x83 0xb4	GEORGIAN SMALL LETTER HAR
					break;
				case 0x2D25:		//	ⴥ	0xe2 0xb4 0xa5	GEORGIAN LETTER HOE
					*p = 0x10F5;	//	ჵ	0xe1 0x83 0xb5	GEORGIAN SMALL LETTER HOE
					break;
				case 0x2D27:		//	ⴧ	0xe2 0xb4 0xa7	GEORGIAN LETTER YN
					*p = 0x10F7;	//	ჷ	0xe1 0x83 0xb7	GEORGIAN SMALL LETTER YN
					break;
				case 0x2D2D:		//	ⴭ	0xe2 0xb4 0xad	GEORGIAN LETTER AEN
					*p = 0x10FD;	//	ჽ	0xe1 0x83 0xbd	GEORGIAN SMALL LETTER AEN
					break;
				case 0xA640:		//	Ꙁ	0xea 0x99 0x80	CYRILLIC CAPITAL LETTER ZEMLYA
					*p = 0xA641;	//	ꙁ	0xea 0x99 0x81	CYRILLIC SMALL LETTER ZEMLYA
					break;
				case 0xA642:		//	Ꙃ	0xea 0x99 0x82	CYRILLIC CAPITAL LETTER DZELO
					*p = 0xA643;	//	ꙃ	0xea 0x99 0x83	CYRILLIC SMALL LETTER DZELO
					break;
				case 0xA644:		//	Ꙅ	0xea 0x99 0x84	CYRILLIC CAPITAL LETTER REVERSED DZE
					*p = 0xA645;	//	ꙅ	0xea 0x99 0x85	CYRILLIC SMALL LETTER REVERSED DZE
					break;
				case 0xA646:		//	Ꙇ	0xea 0x99 0x86	CYRILLIC CAPITAL LETTER IOTA
					*p = 0xA647;	//	ꙇ	0xea 0x99 0x87	CYRILLIC SMALL LETTER IOTA
					break;
				case 0xA648:		//	Ꙉ	0xea 0x99 0x88	CYRILLIC CAPITAL LETTER DJERV
					*p = 0xA649;	//	ꙉ	0xea 0x99 0x89	CYRILLIC SMALL LETTER DJERV
					break;
				case 0xA64A:		//	Ꙋ	0xea 0x99 0x8a	CYRILLIC CAPITAL LETTER MONOGRAPH UK
					*p = 0xA64B;	//	ꙋ	0xea 0x99 0x8b	CYRILLIC SMALL LETTER MONOGRAPH UK
					break;
				case 0xA64C:		//	Ꙍ	0xea 0x99 0x8c	CYRILLIC CAPITAL LETTER BROAD OMEGA
					*p = 0xA64D;	//	ꙍ	0xea 0x99 0x8d	CYRILLIC SMALL LETTER BROAD OMEGA
					break;
				case 0xA64E:		//	Ꙏ	0xea 0x99 0x8e	CYRILLIC CAPITAL LETTER NEUTRAL YER
					*p = 0xA64F;	//	ꙏ	0xea 0x99 0x8f	CYRILLIC SMALL LETTER NEUTRAL YER
					break;
				case 0xA650:		//	Ꙑ	0xea 0x99 0x90	CYRILLIC CAPITAL LETTER YERU WITH BACK YER
					*p = 0xA651;	//	ꙑ	0xea 0x99 0x91	CYRILLIC SMALL LETTER YERU WITH BACK YER
					break;
				case 0xA652:		//	Ꙓ	0xea 0x99 0x92	CYRILLIC CAPITAL LETTER IOTIFIED YAT
					*p = 0xA653;	//	ꙓ	0xea 0x99 0x93	CYRILLIC SMALL LETTER IOTIFIED YAT
					break;
				case 0xA654:		//	Ꙕ	0xea 0x99 0x94	CYRILLIC CAPITAL LETTER REVERSED YU
					*p = 0xA655;	//	ꙕ	0xea 0x99 0x95	CYRILLIC SMALL LETTER REVERSED YU
					break;
				case 0xA656:		//	Ꙗ	0xea 0x99 0x96	CYRILLIC CAPITAL LETTER IOTIFIED A
					*p = 0xA657;	//	ꙗ	0xea 0x99 0x97	CYRILLIC SMALL LETTER IOTIFIED A
					break;
				case 0xA658:		//	Ꙙ	0xea 0x99 0x98	CYRILLIC CAPITAL LETTER CLOSED LITTLE YUS
					*p = 0xA659;	//	ꙙ	0xea 0x99 0x99	CYRILLIC SMALL LETTER CLOSED LITTLE YUS
					break;
				case 0xA65A:		//	Ꙛ	0xea 0x99 0x9a	CYRILLIC CAPITAL LETTER BLENDED YUS
					*p = 0xA65B;	//	ꙛ	0xea 0x99 0x9b	CYRILLIC SMALL LETTER BLENDED YUS
					break;
				case 0xA65C:		//	Ꙝ	0xea 0x99 0x9c	CYRILLIC CAPITAL LETTER IOTIFIED CLOSED LITTLE YUS
					*p = 0xA65D;	//	ꙝ	0xea 0x99 0x9d	CYRILLIC SMALL LETTER IOTIFIED CLOSED LITTLE YUS
					break;
				case 0xA65E:		//	Ꙟ	0xea 0x99 0x9e	CYRILLIC CAPITAL LETTER YN
					*p = 0xA65F;	//	ꙟ	0xea 0x99 0x9f	CYRILLIC SMALL LETTER YN
					break;
				case 0xA660:		//	Ꙡ	0xea 0x99 0xa0	CYRILLIC CAPITAL LETTER REVERSED TSE
					*p = 0xA661;	//	ꙡ	0xea 0x99 0xa1	CYRILLIC SMALL LETTER REVERSED TSE
					break;
				case 0xA662:		//	Ꙣ	0xea 0x99 0xa2	CYRILLIC CAPITAL LETTER SOFT DE
					*p = 0xA663;	//	ꙣ	0xea 0x99 0xa3	CYRILLIC SMALL LETTER SOFT DE
					break;
				case 0xA664:		//	Ꙥ	0xea 0x99 0xa4	CYRILLIC CAPITAL LETTER SOFT EL
					*p = 0xA665;	//	ꙥ	0xea 0x99 0xa5	CYRILLIC SMALL LETTER SOFT EL
					break;
				case 0xA666:		//	Ꙧ	0xea 0x99 0xa6	CYRILLIC CAPITAL LETTER SOFT EM
					*p = 0xA667;	//	ꙧ	0xea 0x99 0xa7	CYRILLIC SMALL LETTER SOFT EM
					break;
				case 0xA668:		//	Ꙩ	0xea 0x99 0xa8	CYRILLIC CAPITAL LETTER MONOCULAR O
					*p = 0xA669;	//	ꙩ	0xea 0x99 0xa9	CYRILLIC SMALL LETTER MONOCULAR O
					break;
				case 0xA66A:		//	Ꙫ	0xea 0x99 0xaa	CYRILLIC CAPITAL LETTER BINOCULAR O
					*p = 0xA66B;	//	ꙫ	0xea 0x99 0xab	CYRILLIC SMALL LETTER BINOCULAR O
					break;
				case 0xA66C:		//	Ꙭ	0xea 0x99 0xac	CYRILLIC CAPITAL LETTER DOUBLE MONOCULAR O
					*p = 0xA66D;	//	ꙭ	0xea 0x99 0xad	CYRILLIC SMALL LETTER DOUBLE MONOCULAR O
					break;
				case 0xA680:		//	Ꚁ	0xea 0x9a 0x80	CYRILLIC CAPITAL LETTER DWE
					*p = 0xA681;	//	ꚁ	0xea 0x9a 0x81	CYRILLIC SMALL LETTER DWE
					break;
				case 0xA682:		//	Ꚃ	0xea 0x9a 0x82	CYRILLIC CAPITAL LETTER DZWE
					*p = 0xA683;	//	ꚃ	0xea 0x9a 0x83	CYRILLIC SMALL LETTER DZWE
					break;
				case 0xA684:		//	Ꚅ	0xea 0x9a 0x84	CYRILLIC CAPITAL LETTER ZHWE
					*p = 0xA685;	//	ꚅ	0xea 0x9a 0x85	CYRILLIC SMALL LETTER ZHWE
					break;
				case 0xA686:		//	Ꚇ	0xea 0x9a 0x86	CYRILLIC CAPITAL LETTER CCHE
					*p = 0xA687;	//	ꚇ	0xea 0x9a 0x87	CYRILLIC SMALL LETTER CCHE
					break;
				case 0xA688:		//	Ꚉ	0xea 0x9a 0x88	CYRILLIC CAPITAL LETTER DZZE
					*p = 0xA689;	//	ꚉ	0xea 0x9a 0x89	CYRILLIC SMALL LETTER DZZE
					break;
				case 0xA68A:		//	Ꚋ	0xea 0x9a 0x8a	CYRILLIC CAPITAL LETTER TE WITH MIDDLE HOOK
					*p = 0xA68B;	//	ꚋ	0xea 0x9a 0x8b	CYRILLIC SMALL LETTER TE WITH MIDDLE HOOK
					break;
				case 0xA68C:		//	Ꚍ	0xea 0x9a 0x8c	CYRILLIC CAPITAL LETTER TWE
					*p = 0xA68D;	//	ꚍ	0xea 0x9a 0x8d	CYRILLIC SMALL LETTER TWE
					break;
				case 0xA68E:		//	Ꚏ	0xea 0x9a 0x8e	CYRILLIC CAPITAL LETTER TSWE
					*p = 0xA68F;	//	ꚏ	0xea 0x9a 0x8f	CYRILLIC SMALL LETTER TSWE
					break;
				case 0xA690:		//	Ꚑ	0xea 0x9a 0x90	CYRILLIC CAPITAL LETTER TSSE
					*p = 0xA691;	//	ꚑ	0xea 0x9a 0x91	CYRILLIC SMALL LETTER TSSE
					break;
				case 0xA692:		//	Ꚓ	0xea 0x9a 0x92	CYRILLIC CAPITAL LETTER TCHE
					*p = 0xA693;	//	ꚓ	0xea 0x9a 0x93	CYRILLIC SMALL LETTER TCHE
					break;
				case 0xA694:		//	Ꚕ	0xea 0x9a 0x94	CYRILLIC CAPITAL LETTER HWE
					*p = 0xA695;	//	ꚕ	0xea 0x9a 0x95	CYRILLIC SMALL LETTER HWE
					break;
				case 0xA696:		//	Ꚗ	0xea 0x9a 0x96	CYRILLIC CAPITAL LETTER SHWE
					*p = 0xA697;	//	ꚗ	0xea 0x9a 0x97	CYRILLIC SMALL LETTER SHWE
					break;
				case 0xA698:		//	Ꚙ	0xea 0x9a 0x98	CYRILLIC CAPITAL LETTER DOUBLE O
					*p = 0xA699;	//	ꚙ	0xea 0x9a 0x99	CYRILLIC SMALL LETTER DOUBLE O
					break;
				case 0xA69A:		//	Ꚛ	0xea 0x9a 0x9a	CYRILLIC CAPITAL LETTER CROSSED O
					*p = 0xA69B;	//	ꚛ	0xea 0x9a 0x9b	CYRILLIC SMALL LETTER CROSSED O
					break;
				case 0xA722:		//	Ꜣ	0xea 0x9c 0xa2	LATIN CAPITAL LETTER EGYPTOLOGICAL ALEF
					*p = 0xA723;	//	ꜣ	0xea 0x9c 0xa3	LATIN SMALL LETTER EGYPTOLOGICAL ALEF
					break;
				case 0xA724:		//	Ꜥ	0xea 0x9c 0xa4	LATIN CAPITAL LETTER EGYPTOLOGICAL AIN
					*p = 0xA725;	//	ꜥ	0xea 0x9c 0xa5	LATIN SMALL LETTER EGYPTOLOGICAL AIN
					break;
				case 0xA726:		//	Ꜧ	0xea 0x9c 0xa6	LATIN CAPITAL LETTER HENG
					*p = 0xA727;	//	ꜧ	0xea 0x9c 0xa7	LATIN SMALL LETTER HENG
					break;
				case 0xA728:		//	Ꜩ	0xea 0x9c 0xa8	LATIN CAPITAL LETTER TZ
					*p = 0xA729;	//	ꜩ	0xea 0x9c 0xa9	LATIN SMALL LETTER TZ
					break;
				case 0xA72A:		//	Ꜫ	0xea 0x9c 0xaa	LATIN CAPITAL LETTER TRESILLO
					*p = 0xA72B;	//	ꜫ	0xea 0x9c 0xab	LATIN SMALL LETTER TRESILLO
					break;
				case 0xA72C:		//	Ꜭ	0xea 0x9c 0xac	LATIN CAPITAL LETTER CUATRILLO
					*p = 0xA72D;	//	ꜭ	0xea 0x9c 0xad	LATIN SMALL LETTER CUATRILLO
					break;
				case 0xA72E:		//	Ꜯ	0xea 0x9c 0xae	LATIN CAPITAL LETTER CUATRILLO WITH COMMA
					*p = 0xA72F;	//	ꜯ	0xea 0x9c 0xaf	LATIN SMALL LETTER CUATRILLO WITH COMMA
					break;
				case 0xA732:		//	Ꜳ	0xea 0x9c 0xb2	LATIN CAPITAL LETTER AA
					*p = 0xA733;	//	ꜳ	0xea 0x9c 0xb3	LATIN SMALL LETTER AA
					break;
				case 0xA734:		//	Ꜵ	0xea 0x9c 0xb4	LATIN CAPITAL LETTER AO
					*p = 0xA735;	//	ꜵ	0xea 0x9c 0xb5	LATIN SMALL LETTER AO
					break;
				case 0xA736:		//	Ꜷ	0xea 0x9c 0xb6	LATIN CAPITAL LETTER AU
					*p = 0xA737;	//	ꜷ	0xea 0x9c 0xb7	LATIN SMALL LETTER AU
					break;
				case 0xA738:		//	Ꜹ	0xea 0x9c 0xb8	LATIN CAPITAL LETTER AV
					*p = 0xA739;	//	ꜹ	0xea 0x9c 0xb9	LATIN SMALL LETTER AV
					break;
				case 0xA73A:		//	Ꜻ	0xea 0x9c 0xba	LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR
					*p = 0xA73B;	//	ꜻ	0xea 0x9c 0xbb	LATIN SMALL LETTER AV WITH HORIZONTAL BAR
					break;
				case 0xA73C:		//	Ꜽ	0xea 0x9c 0xbc	LATIN CAPITAL LETTER AY
					*p = 0xA73D;	//	ꜽ	0xea 0x9c 0xbd	LATIN SMALL LETTER AY
					break;
				case 0xA73E:		//	Ꜿ	0xea 0x9c 0xbe	LATIN CAPITAL LETTER REVERSED C WITH DOT
					*p = 0xA73F;	//	ꜿ	0xea 0x9c 0xbf	LATIN SMALL LETTER REVERSED C WITH DOT
					break;
				case 0xA740:		//	Ꝁ	0xea 0x9d 0x80	LATIN CAPITAL LETTER K WITH STROKE
					*p = 0xA741;	//	ꝁ	0xea 0x9d 0x81	LATIN SMALL LETTER K WITH STROKE
					break;
				case 0xA742:		//	Ꝃ	0xea 0x9d 0x82	LATIN CAPITAL LETTER K WITH DIAGONAL STROKE
					*p = 0xA743;	//	ꝃ	0xea 0x9d 0x83	LATIN SMALL LETTER K WITH DIAGONAL STROKE
					break;
				case 0xA744:		//	Ꝅ	0xea 0x9d 0x84	LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE
					*p = 0xA745;	//	ꝅ	0xea 0x9d 0x85	LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE
					break;
				case 0xA746:		//	Ꝇ	0xea 0x9d 0x86	LATIN CAPITAL LETTER BROKEN L
					*p = 0xA747;	//	ꝇ	0xea 0x9d 0x87	LATIN SMALL LETTER BROKEN L
					break;
				case 0xA748:		//	Ꝉ	0xea 0x9d 0x88	LATIN CAPITAL LETTER L WITH HIGH STROKE
					*p = 0xA749;	//	ꝉ	0xea 0x9d 0x89	LATIN SMALL LETTER L WITH HIGH STROKE
					break;
				case 0xA74A:		//	Ꝋ	0xea 0x9d 0x8a	LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY
					*p = 0xA74B;	//	ꝋ	0xea 0x9d 0x8b	LATIN SMALL LETTER O WITH LONG STROKE OVERLAY
					break;
				case 0xA74C:		//	Ꝍ	0xea 0x9d 0x8c	LATIN CAPITAL LETTER O WITH LOOP
					*p = 0xA74D;	//	ꝍ	0xea 0x9d 0x8d	LATIN SMALL LETTER O WITH LOOP
					break;
				case 0xA74E:		//	Ꝏ	0xea 0x9d 0x8e	LATIN CAPITAL LETTER OO
					*p = 0xA74F;	//	ꝏ	0xea 0x9d 0x8f	LATIN SMALL LETTER OO
					break;
				case 0xA750:		//	Ꝑ	0xea 0x9d 0x90	LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER
					*p = 0xA751;	//	ꝑ	0xea 0x9d 0x91	LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER
					break;
				case 0xA752:		//	Ꝓ	0xea 0x9d 0x92	LATIN CAPITAL LETTER P WITH FLOURISH
					*p = 0xA753;	//	ꝓ	0xea 0x9d 0x93	LATIN SMALL LETTER P WITH FLOURISH
					break;
				case 0xA754:		//	Ꝕ	0xea 0x9d 0x94	LATIN CAPITAL LETTER P WITH SQUIRREL TAIL
					*p = 0xA755;	//	ꝕ	0xea 0x9d 0x95	LATIN SMALL LETTER P WITH SQUIRREL TAIL
					break;
				case 0xA756:		//	Ꝗ	0xea 0x9d 0x96	LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER
					*p = 0xA757;	//	ꝗ	0xea 0x9d 0x97	LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER
					break;
				case 0xA758:		//	Ꝙ	0xea 0x9d 0x98	LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE
					*p = 0xA759;	//	ꝙ	0xea 0x9d 0x99	LATIN SMALL LETTER Q WITH DIAGONAL STROKE
					break;
				case 0xA75A:		//	Ꝛ	0xea 0x9d 0x9a	LATIN CAPITAL LETTER R ROTUNDA
					*p = 0xA75B;	//	ꝛ	0xea 0x9d 0x9b	LATIN SMALL LETTER R ROTUNDA
					break;
				case 0xA75C:		//	Ꝝ	0xea 0x9d 0x9c	LATIN CAPITAL LETTER RUM ROTUNDA
					*p = 0xA75D;	//	ꝝ	0xea 0x9d 0x9d	LATIN SMALL LETTER RUM ROTUNDA
					break;
				case 0xA75E:		//	Ꝟ	0xea 0x9d 0x9e	LATIN CAPITAL LETTER V WITH DIAGONAL STROKE
					*p = 0xA75F;	//	ꝟ	0xea 0x9d 0x9f	LATIN SMALL LETTER V WITH DIAGONAL STROKE
					break;
				case 0xA760:		//	Ꝡ	0xea 0x9d 0xa0	LATIN CAPITAL LETTER VY
					*p = 0xA761;	//	ꝡ	0xea 0x9d 0xa1	LATIN SMALL LETTER VY
					break;
				case 0xA762:		//	Ꝣ	0xea 0x9d 0xa2	LATIN CAPITAL LETTER VISIGOTHIC Z
					*p = 0xA763;	//	ꝣ	0xea 0x9d 0xa3	LATIN SMALL LETTER VISIGOTHIC Z
					break;
				case 0xA764:		//	Ꝥ	0xea 0x9d 0xa4	LATIN CAPITAL LETTER THORN WITH STROKE
					*p = 0xA765;	//	ꝥ	0xea 0x9d 0xa5	LATIN SMALL LETTER THORN WITH STROKE
					break;
				case 0xA766:		//	Ꝧ	0xea 0x9d 0xa6	LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER
					*p = 0xA767;	//	ꝧ	0xea 0x9d 0xa7	LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER
					break;
				case 0xA768:		//	Ꝩ	0xea 0x9d 0xa8	LATIN CAPITAL LETTER VEND
					*p = 0xA769;	//	ꝩ	0xea 0x9d 0xa9	LATIN SMALL LETTER VEND
					break;
				case 0xA76A:		//	Ꝫ	0xea 0x9d 0xaa	LATIN CAPITAL LETTER ET
					*p = 0xA76B;	//	ꝫ	0xea 0x9d 0xab	LATIN SMALL LETTER ET
					break;
				case 0xA76C:		//	Ꝭ	0xea 0x9d 0xac	LATIN CAPITAL LETTER IS
					*p = 0xA76D;	//	ꝭ	0xea 0x9d 0xad	LATIN SMALL LETTER IS
					break;
				case 0xA76E:		//	Ꝯ	0xea 0x9d 0xae	LATIN CAPITAL LETTER CON
					*p = 0xA76F;	//	ꝯ	0xea 0x9d 0xaf	LATIN SMALL LETTER CON
					break;
				case 0xA779:		//	Ꝺ	0xea 0x9d 0xb9	LATIN CAPITAL LETTER INSULAR D
					*p = 0xA77A;	//	ꝺ	0xea 0x9d 0xba	LATIN SMALL LETTER INSULAR D
					break;
				case 0xA77B:		//	Ꝼ	0xea 0x9d 0xbb	LATIN CAPITAL LETTER INSULAR F
					*p = 0xA77C;	//	ꝼ	0xea 0x9d 0xbc	LATIN SMALL LETTER INSULAR F
					break;
				case 0xA77D:		//	Ᵹ	0xea 0x9d 0xbd	LATIN CAPITAL LETTER INSULAR G
					*p = 0x1D79;	//	ᵹ	0xe1 0xb5 0xb9	LATIN SMALL LETTER INSULAR G
					break;
				case 0xA77E:		//	Ꝿ	0xea 0x9d 0xbe	LATIN CAPITAL LETTER TURNED INSULAR G
					*p = 0xA77F;	//	ꝿ	0xea 0x9d 0xbf	LATIN SMALL LETTER TURNED INSULAR G
					break;
				case 0xA780:		//	Ꞁ	0xea 0x9e 0x80	LATIN CAPITAL LETTER TURNED L
					*p = 0xA781;	//	ꞁ	0xea 0x9e 0x81	LATIN SMALL LETTER TURNED L
					break;
				case 0xA782:		//	Ꞃ	0xea 0x9e 0x82	LATIN CAPITAL LETTER INSULAR R
					*p = 0xA783;	//	ꞃ	0xea 0x9e 0x83	LATIN SMALL LETTER INSULAR R
					break;
				case 0xA784:		//	Ꞅ	0xea 0x9e 0x84	LATIN CAPITAL LETTER INSULAR S
					*p = 0xA785;	//	ꞅ	0xea 0x9e 0x85	LATIN SMALL LETTER INSULAR S
					break;
				case 0xA786:		//	Ꞇ	0xea 0x9e 0x86	LATIN CAPITAL LETTER INSULAR T
					*p = 0xA787;	//	ꞇ	0xea 0x9e 0x87	LATIN SMALL LETTER INSULAR T
					break;
				case 0xA78B:		//	Ꞌ	0xea 0x9e 0x8b	LATIN CAPITAL LETTER SALTILLO
					*p = 0xA78C;	//	ꞌ	0xea 0x9e 0x8c	LATIN SMALL LETTER SALTILLO
					break;
				case 0xA78D:		//	Ɥ	0xea 0x9e 0x8d	LATIN CAPITAL LETTER TURNED H
					*p = 0x0265;	//	ɥ	0xc9 0xa5	LATIN SMALL LETTER TURNED H
					break;
				case 0xA790:		//	Ꞑ	0xea 0x9e 0x90	LATIN CAPITAL LETTER N WITH DESCENDER
					*p = 0xA791;	//	ꞑ	0xea 0x9e 0x91	LATIN SMALL LETTER N WITH DESCENDER
					break;
				case 0xA792:		//	Ꞓ	0xea 0x9e 0x92	LATIN CAPITAL LETTER C WITH BAR
					*p = 0xA793;	//	ꞓ	0xea 0x9e 0x93	LATIN SMALL LETTER C WITH BAR
					break;
				case 0xA796:		//	Ꞗ	0xea 0x9e 0x96	LATIN CAPITAL LETTER B WITH FLOURISH
					*p = 0xA797;	//	ꞗ	0xea 0x9e 0x97	LATIN SMALL LETTER B WITH FLOURISH
					break;
				case 0xA798:		//	Ꞙ	0xea 0x9e 0x98	LATIN CAPITAL LETTER F WITH STROKE
					*p = 0xA799;	//	ꞙ	0xea 0x9e 0x99	LATIN SMALL LETTER F WITH STROKE
					break;
				case 0xA79A:		//	Ꞛ	0xea 0x9e 0x9a	LATIN CAPITAL LETTER VOLAPUK AE
					*p = 0xA79B;	//	ꞛ	0xea 0x9e 0x9b	LATIN SMALL LETTER VOLAPUK AE
					break;
				case 0xA79C:		//	Ꞝ	0xea 0x9e 0x9c	LATIN CAPITAL LETTER VOLAPUK OE
					*p = 0xA79D;	//	ꞝ	0xea 0x9e 0x9d	LATIN SMALL LETTER VOLAPUK OE
					break;
				case 0xA79E:		//	Ꞟ	0xea 0x9e 0x9e	LATIN CAPITAL LETTER VOLAPUK UE
					*p = 0xA79F;	//	ꞟ	0xea 0x9e 0x9f	LATIN SMALL LETTER VOLAPUK UE
					break;
				case 0xA7A0:		//	Ꞡ	0xea 0x9e 0xa0	LATIN CAPITAL LETTER G WITH OBLIQUE STROKE
					*p = 0xA7A1;	//	ꞡ	0xea 0x9e 0xa1	LATIN SMALL LETTER G WITH OBLIQUE STROKE
					break;
				case 0xA7A2:		//	Ꞣ	0xea 0x9e 0xa2	LATIN CAPITAL LETTER K WITH OBLIQUE STROKE
					*p = 0xA7A3;	//	ꞣ	0xea 0x9e 0xa3	LATIN SMALL LETTER K WITH OBLIQUE STROKE
					break;
				case 0xA7A4:		//	Ꞥ	0xea 0x9e 0xa4	LATIN CAPITAL LETTER N WITH OBLIQUE STROKE
					*p = 0xA7A5;	//	ꞥ	0xea 0x9e 0xa5	LATIN SMALL LETTER N WITH OBLIQUE STROKE
					break;
				case 0xA7A6:		//	Ꞧ	0xea 0x9e 0xa6	LATIN CAPITAL LETTER R WITH OBLIQUE STROKE
					*p = 0xA7A7;	//	ꞧ	0xea 0x9e 0xa7	LATIN SMALL LETTER R WITH OBLIQUE STROKE
					break;
				case 0xA7A8:		//	Ꞩ	0xea 0x9e 0xa8	LATIN CAPITAL LETTER S WITH OBLIQUE STROKE
					*p = 0xA7A9;	//	ꞩ	0xea 0x9e 0xa9	LATIN SMALL LETTER S WITH OBLIQUE STROKE
					break;
				case 0xA7AA:		//	Ɦ	0xea 0x9e 0xaa	LATIN CAPITAL LETTER H WITH HOOK
					*p = 0x0266;	//	ɦ	0xc9 0xa6	LATIN SMALL LETTER H WITH HOOK
					break;
				case 0xA7AB:		//	Ɜ	0xea 0x9e 0xab	LATIN CAPITAL LETTER REVERSED OPEN E
					*p = 0x025C;	//	ɜ	0xc9 0x9c	LATIN SMALL LETTER REVERSED OPEN E
					break;
				case 0xA7AC:		//	Ɡ	0xea 0x9e 0xac	LATIN CAPITAL LETTER SCRIPT G
					*p = 0x0261;	//	ɡ	0xc9 0xa1	LATIN SMALL LETTER SCRIPT G
					break;
				case 0xA7AD:		//	Ɬ	0xea 0x9e 0xad	LATIN CAPITAL LETTER L WITH BELT
					*p = 0x026C;	//	ɬ	0xc9 0xac	LATIN SMALL LETTER L WITH BELT
					break;
				case 0xA7AE:		//	Ɪ	0xea 0x9e 0xae	LATIN CAPITAL LETTER SMALL CAPITAL I
					*p = 0x026A;	//	ɪ	0xc9 0xaa	LATIN SMALL LETTER SMALL CAPITAL I
					break;
				case 0xA7B0:		//	Ʞ	0xea 0x9e 0xb0	LATIN CAPITAL LETTER TURNED K
					*p = 0x029E;	//	ʞ	0xca 0x9e	LATIN SMALL LETTER TURNED K
					break;
				case 0xA7B1:		//	Ʇ	0xea 0x9e 0xb1	LATIN CAPITAL LETTER TURNED T
					*p = 0x0287;	//	ʇ	0xca 0x87	LATIN SMALL LETTER TURNED T
					break;
				case 0xA7B2:		//	Ʝ	0xea 0x9e 0xb2	LATIN CAPITAL LETTER J WITH CROSSED-TAIL
					*p = 0x029D;	//	ʝ	0xca 0x9d	LATIN SMALL LETTER J WITH CROSSED-TAIL
					break;
				case 0xA7B3:		//	Ꭓ	0xea 0x9e 0xb3	LATIN CAPITAL LETTER CHI
					*p = 0xAB53;	//	ꭓ	0xea 0xad 0x93	LATIN SMALL LETTER CHI
					break;
				case 0xA7B4:		//	Ꞵ	0xea 0x9e 0xb4	LATIN CAPITAL LETTER BETA
					*p = 0xA7B5;	//	ꞵ	0xea 0x9e 0xb5	LATIN SMALL LETTER BETA
					break;
				case 0xA7B6:		//	Ꞷ	0xea 0x9e 0xb6	LATIN CAPITAL LETTER OMEGA
					*p = 0xA7B7;	//	ꞷ	0xea 0x9e 0xb7	LATIN SMALL LETTER OMEGA
					break;
				case 0xA7B8:		//	Ꞹ	0xea 0x9e 0xb8	LATIN CAPITAL LETTER U WITH STROKE
					*p = 0xA7B9;	//	ꞹ	0xea 0x9e 0xb9	LATIN SMALL LETTER U WITH STROKE
					break;
				case 0xA7BA:		//	Ꞻ	0xea 0x9e 0xba	LATIN CAPITAL LETTER GLOTTAL A
					*p = 0xA7BB;	//	ꞻ	0xea 0x9e 0xbb	LATIN SMALL LETTER GLOTTAL A
					break;
				case 0xA7BC:		//	Ꞽ	0xea 0x9e 0xbc	LATIN CAPITAL LETTER GLOTTAL I
					*p = 0xA7BD;	//	ꞽ	0xea 0x9e 0xbd	LATIN SMALL LETTER GLOTTAL I
					break;
				case 0xA7BE:		//	Ꞿ	0xea 0x9e 0xbe	LATIN CAPITAL LETTER GLOTTAL U
					*p = 0xA7BF;	//	ꞿ	0xea 0x9e 0xbf	LATIN SMALL LETTER GLOTTAL U
					break;
				case 0xA7C2:		//	Ꟃ	0xea 0x9f 0x82	LATIN CAPITAL LETTER ANGLICANA W
					*p = 0xA7C3;	//	ꟃ	0xea 0x9f 0x83	LATIN SMALL LETTER ANGLICANA W
					break;
				case 0xA7C4:		//	Ꞔ	0xea 0x9f 0x84	LATIN CAPITAL LETTER C WITH PALATAL HOOK
					*p = 0xA794;	//	ꞔ	0xea 0x9e 0x94	LATIN SMALL LETTER C WITH PALATAL HOOK
					break;
				case 0xA7C5:		//	Ʂ	0xea 0x9f 0x85	LATIN CAPITAL LETTER S WITH HOOK
					*p = 0x0282;	//	ʂ	0xca 0x82	LATIN SMALL LETTER S WITH HOOK
					break;
				case 0xA7C6:		//	Ᶎ	0xea 0x9f 0x86	LATIN CAPITAL LETTER Z WITH PALATAL HOOK
					*p = 0x1D8E;	//	ᶎ	0xe1 0xb6 0x8e	LATIN SMALL LETTER Z WITH PALATAL HOOK
					break;
				case 0xA7C7:		//	Ꟈ	0xea 0x9f 0x87	LATIN CAPITAL LETTER D WITH SHORT STROKE OVERLAY
					*p = 0xA7C8;	//	ꟈ	0xea 0x9f 0x88	LATIN SMALL LETTER D WITH SHORT STROKE OVERLAY
					break;
				case 0xA7C9:		//	Ꟊ	0xea 0x9f 0x89	LATIN CAPITAL LETTER S WITH SHORT STROKE OVERLAY
					*p = 0xA7CA;	//	ꟊ	0xea 0x9f 0x8a	LATIN SMALL LETTER S WITH SHORT STROKE OVERLAY
					break;
				case 0xA7F5:		//	Ꟶ	0xea 0x9f 0xb5	LATIN CAPITAL LETTER REVERSED HALF H
					*p = 0xA7F6;	//	ꟶ	0xea 0x9f 0xb6	LATIN SMALL LETTER REVERSED HALF H
					break;
				case 0xFF21:		//	Ａ	0xef 0xbc 0xa1	FULLWIDTH LATIN CAPITAL LETTER A
					*p = 0xFF41;	//	ａ	0xef 0xbd 0x81	FULLWIDTH LATIN SMALL LETTER A
					break;
				case 0xFF22:		//	Ｂ	0xef 0xbc 0xa2	FULLWIDTH LATIN CAPITAL LETTER B
					*p = 0xFF42;	//	ｂ	0xef 0xbd 0x82	FULLWIDTH LATIN SMALL LETTER B
					break;
				case 0xFF23:		//	Ｃ	0xef 0xbc 0xa3	FULLWIDTH LATIN CAPITAL LETTER C
					*p = 0xFF43;	//	ｃ	0xef 0xbd 0x83	FULLWIDTH LATIN SMALL LETTER C
					break;
				case 0xFF24:		//	Ｄ	0xef 0xbc 0xa4	FULLWIDTH LATIN CAPITAL LETTER D
					*p = 0xFF44;	//	ｄ	0xef 0xbd 0x84	FULLWIDTH LATIN SMALL LETTER D
					break;
				case 0xFF25:		//	Ｅ	0xef 0xbc 0xa5	FULLWIDTH LATIN CAPITAL LETTER E
					*p = 0xFF45;	//	ｅ	0xef 0xbd 0x85	FULLWIDTH LATIN SMALL LETTER E
					break;
				case 0xFF26:		//	Ｆ	0xef 0xbc 0xa6	FULLWIDTH LATIN CAPITAL LETTER F
					*p = 0xFF46;	//	ｆ	0xef 0xbd 0x86	FULLWIDTH LATIN SMALL LETTER F
					break;
				case 0xFF27:		//	Ｇ	0xef 0xbc 0xa7	FULLWIDTH LATIN CAPITAL LETTER G
					*p = 0xFF47;	//	ｇ	0xef 0xbd 0x87	FULLWIDTH LATIN SMALL LETTER G
					break;
				case 0xFF28:		//	Ｈ	0xef 0xbc 0xa8	FULLWIDTH LATIN CAPITAL LETTER H
					*p = 0xFF48;	//	ｈ	0xef 0xbd 0x88	FULLWIDTH LATIN SMALL LETTER H
					break;
				case 0xFF29:		//	Ｉ	0xef 0xbc 0xa9	FULLWIDTH LATIN CAPITAL LETTER I
					*p = 0xFF49;	//	ｉ	0xef 0xbd 0x89	FULLWIDTH LATIN SMALL LETTER I
					break;
				case 0xFF2A:		//	Ｊ	0xef 0xbc 0xaa	FULLWIDTH LATIN CAPITAL LETTER J
					*p = 0xFF4A;	//	ｊ	0xef 0xbd 0x8a	FULLWIDTH LATIN SMALL LETTER J
					break;
				case 0xFF2B:		//	Ｋ	0xef 0xbc 0xab	FULLWIDTH LATIN CAPITAL LETTER K
					*p = 0xFF4B;	//	ｋ	0xef 0xbd 0x8b	FULLWIDTH LATIN SMALL LETTER K
					break;
				case 0xFF2C:		//	Ｌ	0xef 0xbc 0xac	FULLWIDTH LATIN CAPITAL LETTER L
					*p = 0xFF4C;	//	ｌ	0xef 0xbd 0x8c	FULLWIDTH LATIN SMALL LETTER L
					break;
				case 0xFF2D:		//	Ｍ	0xef 0xbc 0xad	FULLWIDTH LATIN CAPITAL LETTER M
					*p = 0xFF4D;	//	ｍ	0xef 0xbd 0x8d	FULLWIDTH LATIN SMALL LETTER M
					break;
				case 0xFF2E:		//	Ｎ	0xef 0xbc 0xae	FULLWIDTH LATIN CAPITAL LETTER N
					*p = 0xFF4E;	//	ｎ	0xef 0xbd 0x8e	FULLWIDTH LATIN SMALL LETTER N
					break;
				case 0xFF2F:		//	Ｏ	0xef 0xbc 0xaf	FULLWIDTH LATIN CAPITAL LETTER O
					*p = 0xFF4F;	//	ｏ	0xef 0xbd 0x8f	FULLWIDTH LATIN SMALL LETTER O
					break;
				case 0xFF30:		//	Ｐ	0xef 0xbc 0xb0	FULLWIDTH LATIN CAPITAL LETTER P
					*p = 0xFF50;	//	ｐ	0xef 0xbd 0x90	FULLWIDTH LATIN SMALL LETTER P
					break;
				case 0xFF31:		//	Ｑ	0xef 0xbc 0xb1	FULLWIDTH LATIN CAPITAL LETTER Q
					*p = 0xFF51;	//	ｑ	0xef 0xbd 0x91	FULLWIDTH LATIN SMALL LETTER Q
					break;
				case 0xFF32:		//	Ｒ	0xef 0xbc 0xb2	FULLWIDTH LATIN CAPITAL LETTER R
					*p = 0xFF52;	//	ｒ	0xef 0xbd 0x92	FULLWIDTH LATIN SMALL LETTER R
					break;
				case 0xFF33:		//	Ｓ	0xef 0xbc 0xb3	FULLWIDTH LATIN CAPITAL LETTER S
					*p = 0xFF53;	//	ｓ	0xef 0xbd 0x93	FULLWIDTH LATIN SMALL LETTER S
					break;
				case 0xFF34:		//	Ｔ	0xef 0xbc 0xb4	FULLWIDTH LATIN CAPITAL LETTER T
					*p = 0xFF54;	//	ｔ	0xef 0xbd 0x94	FULLWIDTH LATIN SMALL LETTER T
					break;
				case 0xFF35:		//	Ｕ	0xef 0xbc 0xb5	FULLWIDTH LATIN CAPITAL LETTER U
					*p = 0xFF55;	//	ｕ	0xef 0xbd 0x95	FULLWIDTH LATIN SMALL LETTER U
					break;
				case 0xFF36:		//	Ｖ	0xef 0xbc 0xb6	FULLWIDTH LATIN CAPITAL LETTER V
					*p = 0xFF56;	//	ｖ	0xef 0xbd 0x96	FULLWIDTH LATIN SMALL LETTER V
					break;
				case 0xFF37:		//	Ｗ	0xef 0xbc 0xb7	FULLWIDTH LATIN CAPITAL LETTER W
					*p = 0xFF57;	//	ｗ	0xef 0xbd 0x97	FULLWIDTH LATIN SMALL LETTER W
					break;
				case 0xFF38:		//	Ｘ	0xef 0xbc 0xb8	FULLWIDTH LATIN CAPITAL LETTER X
					*p = 0xFF58;	//	ｘ	0xef 0xbd 0x98	FULLWIDTH LATIN SMALL LETTER X
					break;
				case 0xFF39:		//	Ｙ	0xef 0xbc 0xb9	FULLWIDTH LATIN CAPITAL LETTER Y
					*p = 0xFF59;	//	ｙ	0xef 0xbd 0x99	FULLWIDTH LATIN SMALL LETTER Y
					break;
				case 0xFF3A:		//	Ｚ	0xef 0xbc 0xba	FULLWIDTH LATIN CAPITAL LETTER Z
					*p = 0xFF5A;	//	ｚ	0xef 0xbd 0x9a	FULLWIDTH LATIN SMALL LETTER Z
					break;
				case 0x10400:		//	𐐀	0xf0 0x90 0x90 0x80	DESERET CAPITAL LETTER LONG I
					*p = 0x10428;	//	𐐨	0xf0 0x90 0x90 0xa8	DESERET SMALL LETTER LONG I
					break;
				case 0x10401:		//	𐐁	0xf0 0x90 0x90 0x81	DESERET CAPITAL LETTER LONG E
					*p = 0x10429;	//	𐐩	0xf0 0x90 0x90 0xa9	DESERET SMALL LETTER LONG E
					break;
				case 0x10402:		//	𐐂	0xf0 0x90 0x90 0x82	DESERET CAPITAL LETTER LONG A
					*p = 0x1042A;	//	𐐪	0xf0 0x90 0x90 0xaa	DESERET SMALL LETTER LONG A
					break;
				case 0x10403:		//	𐐃	0xf0 0x90 0x90 0x83	DESERET CAPITAL LETTER LONG AH
					*p = 0x1042B;	//	𐐫	0xf0 0x90 0x90 0xab	DESERET SMALL LETTER LONG AH
					break;
				case 0x10404:		//	𐐄	0xf0 0x90 0x90 0x84	DESERET CAPITAL LETTER LONG O
					*p = 0x1042C;	//	𐐬	0xf0 0x90 0x90 0xac	DESERET SMALL LETTER LONG O
					break;
				case 0x10405:		//	𐐅	0xf0 0x90 0x90 0x85	DESERET CAPITAL LETTER LONG OO
					*p = 0x1042D;	//	𐐭	0xf0 0x90 0x90 0xad	DESERET SMALL LETTER LONG OO
					break;
				case 0x10406:		//	𐐆	0xf0 0x90 0x90 0x86	DESERET CAPITAL LETTER SHORT I
					*p = 0x1042E;	//	𐐮	0xf0 0x90 0x90 0xae	DESERET SMALL LETTER SHORT I
					break;
				case 0x10407:		//	𐐇	0xf0 0x90 0x90 0x87	DESERET CAPITAL LETTER SHORT E
					*p = 0x1042F;	//	𐐯	0xf0 0x90 0x90 0xaf	DESERET SMALL LETTER SHORT E
					break;
				case 0x10408:		//	𐐈	0xf0 0x90 0x90 0x88	DESERET CAPITAL LETTER SHORT A
					*p = 0x10430;	//	𐐰	0xf0 0x90 0x90 0xb0	DESERET SMALL LETTER SHORT A
					break;
				case 0x10409:		//	𐐉	0xf0 0x90 0x90 0x89	DESERET CAPITAL LETTER SHORT AH
					*p = 0x10431;	//	𐐱	0xf0 0x90 0x90 0xb1	DESERET SMALL LETTER SHORT AH
					break;
				case 0x1040A:		//	𐐊	0xf0 0x90 0x90 0x8a	DESERET CAPITAL LETTER SHORT O
					*p = 0x10432;	//	𐐲	0xf0 0x90 0x90 0xb2	DESERET SMALL LETTER SHORT O
					break;
				case 0x1040B:		//	𐐋	0xf0 0x90 0x90 0x8b	DESERET CAPITAL LETTER SHORT OO
					*p = 0x10433;	//	𐐳	0xf0 0x90 0x90 0xb3	DESERET SMALL LETTER SHORT OO
					break;
				case 0x1040C:		//	𐐌	0xf0 0x90 0x90 0x8c	DESERET CAPITAL LETTER AY
					*p = 0x10434;	//	𐐴	0xf0 0x90 0x90 0xb4	DESERET SMALL LETTER AY
					break;
				case 0x1040D:		//	𐐍	0xf0 0x90 0x90 0x8d	DESERET CAPITAL LETTER OW
					*p = 0x10435;	//	𐐵	0xf0 0x90 0x90 0xb5	DESERET SMALL LETTER OW
					break;
				case 0x1040E:		//	𐐎	0xf0 0x90 0x90 0x8e	DESERET CAPITAL LETTER WU
					*p = 0x10436;	//	𐐶	0xf0 0x90 0x90 0xb6	DESERET SMALL LETTER WU
					break;
				case 0x1040F:		//	𐐏	0xf0 0x90 0x90 0x8f	DESERET CAPITAL LETTER YEE
					*p = 0x10437;	//	𐐷	0xf0 0x90 0x90 0xb7	DESERET SMALL LETTER YEE
					break;
				case 0x10410:		//	𐐐	0xf0 0x90 0x90 0x90	DESERET CAPITAL LETTER H
					*p = 0x10438;	//	𐐸	0xf0 0x90 0x90 0xb8	DESERET SMALL LETTER H
					break;
				case 0x10411:		//	𐐑	0xf0 0x90 0x90 0x91	DESERET CAPITAL LETTER PEE
					*p = 0x10439;	//	𐐹	0xf0 0x90 0x90 0xb9	DESERET SMALL LETTER PEE
					break;
				case 0x10412:		//	𐐒	0xf0 0x90 0x90 0x92	DESERET CAPITAL LETTER BEE
					*p = 0x1043A;	//	𐐺	0xf0 0x90 0x90 0xba	DESERET SMALL LETTER BEE
					break;
				case 0x10413:		//	𐐓	0xf0 0x90 0x90 0x93	DESERET CAPITAL LETTER TEE
					*p = 0x1043B;	//	𐐻	0xf0 0x90 0x90 0xbb	DESERET SMALL LETTER TEE
					break;
				case 0x10414:		//	𐐔	0xf0 0x90 0x90 0x94	DESERET CAPITAL LETTER DEE
					*p = 0x1043C;	//	𐐼	0xf0 0x90 0x90 0xbc	DESERET SMALL LETTER DEE
					break;
				case 0x10415:		//	𐐕	0xf0 0x90 0x90 0x95	DESERET CAPITAL LETTER CHEE
					*p = 0x1043D;	//	𐐽	0xf0 0x90 0x90 0xbd	DESERET SMALL LETTER CHEE
					break;
				case 0x10416:		//	𐐖	0xf0 0x90 0x90 0x96	DESERET CAPITAL LETTER JEE
					*p = 0x1043E;	//	𐐾	0xf0 0x90 0x90 0xbe	DESERET SMALL LETTER JEE
					break;
				case 0x10417:		//	𐐗	0xf0 0x90 0x90 0x97	DESERET CAPITAL LETTER KAY
					*p = 0x1043F;	//	𐐿	0xf0 0x90 0x90 0xbf	DESERET SMALL LETTER KAY
					break;
				case 0x10418:		//	𐐘	0xf0 0x90 0x90 0x98	DESERET CAPITAL LETTER GAY
					*p = 0x10440;	//	𐑀	0xf0 0x90 0x91 0x80	DESERET SMALL LETTER GAY
					break;
				case 0x10419:		//	𐐙	0xf0 0x90 0x90 0x99	DESERET CAPITAL LETTER EF
					*p = 0x10441;	//	𐑁	0xf0 0x90 0x91 0x81	DESERET SMALL LETTER EF
					break;
				case 0x1041A:		//	𐐚	0xf0 0x90 0x90 0x9a	DESERET CAPITAL LETTER VEE
					*p = 0x10442;	//	𐑂	0xf0 0x90 0x91 0x82	DESERET SMALL LETTER VEE
					break;
				case 0x1041B:		//	𐐛	0xf0 0x90 0x90 0x9b	DESERET CAPITAL LETTER ETH
					*p = 0x10443;	//	𐑃	0xf0 0x90 0x91 0x83	DESERET SMALL LETTER ETH
					break;
				case 0x1041C:		//	𐐜	0xf0 0x90 0x90 0x9c	DESERET CAPITAL LETTER THEE
					*p = 0x10444;	//	𐑄	0xf0 0x90 0x91 0x84	DESERET SMALL LETTER THEE
					break;
				case 0x1041D:		//	𐐝	0xf0 0x90 0x90 0x9d	DESERET CAPITAL LETTER ES
					*p = 0x10445;	//	𐑅	0xf0 0x90 0x91 0x85	DESERET SMALL LETTER ES
					break;
				case 0x1041E:		//	𐐞	0xf0 0x90 0x90 0x9e	DESERET CAPITAL LETTER ZEE
					*p = 0x10446;	//	𐑆	0xf0 0x90 0x91 0x86	DESERET SMALL LETTER ZEE
					break;
				case 0x1041F:		//	𐐟	0xf0 0x90 0x90 0x9f	DESERET CAPITAL LETTER ESH
					*p = 0x10447;	//	𐑇	0xf0 0x90 0x91 0x87	DESERET SMALL LETTER ESH
					break;
				case 0x10420:		//	𐐠	0xf0 0x90 0x90 0xa0	DESERET CAPITAL LETTER ZHEE
					*p = 0x10448;	//	𐑈	0xf0 0x90 0x91 0x88	DESERET SMALL LETTER ZHEE
					break;
				case 0x10421:		//	𐐡	0xf0 0x90 0x90 0xa1	DESERET CAPITAL LETTER ER
					*p = 0x10449;	//	𐑉	0xf0 0x90 0x91 0x89	DESERET SMALL LETTER ER
					break;
				case 0x10422:		//	𐐢	0xf0 0x90 0x90 0xa2	DESERET CAPITAL LETTER EL
					*p = 0x1044A;	//	𐑊	0xf0 0x90 0x91 0x8a	DESERET SMALL LETTER EL
					break;
				case 0x10423:		//	𐐣	0xf0 0x90 0x90 0xa3	DESERET CAPITAL LETTER EM
					*p = 0x1044B;	//	𐑋	0xf0 0x90 0x91 0x8b	DESERET SMALL LETTER EM
					break;
				case 0x10424:		//	𐐤	0xf0 0x90 0x90 0xa4	DESERET CAPITAL LETTER EN
					*p = 0x1044C;	//	𐑌	0xf0 0x90 0x91 0x8c	DESERET SMALL LETTER EN
					break;
				case 0x10425:		//	𐐥	0xf0 0x90 0x90 0xa5	DESERET CAPITAL LETTER ENG
					*p = 0x1044D;	//	𐑍	0xf0 0x90 0x91 0x8d	DESERET SMALL LETTER ENG
					break;
				case 0x10426:		//	𐐦	0xf0 0x90 0x90 0xa6	DESERET CAPITAL LETTER OI
					*p = 0x1044E;	//	𐑎	0xf0 0x90 0x91 0x8e	DESERET SMALL LETTER OI
					break;
				case 0x10427:		//	𐐧	0xf0 0x90 0x90 0xa7	DESERET CAPITAL LETTER EW
					*p = 0x1044F;	//	𐑏	0xf0 0x90 0x91 0x8f	DESERET SMALL LETTER EW
					break;
				case 0x104B0:		//	𐒰	0xf0 0x90 0x92 0xb0	OSAGE CAPITAL LETTER A
					*p = 0x104D8;	//	𐓘	0xf0 0x90 0x93 0x98	OSAGE SMALL LETTER A
					break;
				case 0x104B1:		//	𐒱	0xf0 0x90 0x92 0xb1	OSAGE CAPITAL LETTER AI
					*p = 0x104D9;	//	𐓙	0xf0 0x90 0x93 0x99	OSAGE SMALL LETTER AI
					break;
				case 0x104B2:		//	𐒲	0xf0 0x90 0x92 0xb2	OSAGE CAPITAL LETTER AIN
					*p = 0x104DA;	//	𐓚	0xf0 0x90 0x93 0x9a	OSAGE SMALL LETTER AIN
					break;
				case 0x104B3:		//	𐒳	0xf0 0x90 0x92 0xb3	OSAGE CAPITAL LETTER AH
					*p = 0x104DB;	//	𐓛	0xf0 0x90 0x93 0x9b	OSAGE SMALL LETTER AH
					break;
				case 0x104B4:		//	𐒴	0xf0 0x90 0x92 0xb4	OSAGE CAPITAL LETTER BRA
					*p = 0x104DC;	//	𐓜	0xf0 0x90 0x93 0x9c	OSAGE SMALL LETTER BRA
					break;
				case 0x104B5:		//	𐒵	0xf0 0x90 0x92 0xb5	OSAGE CAPITAL LETTER CHA
					*p = 0x104DD;	//	𐓝	0xf0 0x90 0x93 0x9d	OSAGE SMALL LETTER CHA
					break;
				case 0x104B6:		//	𐒶	0xf0 0x90 0x92 0xb6	OSAGE CAPITAL LETTER EHCHA
					*p = 0x104DE;	//	𐓞	0xf0 0x90 0x93 0x9e	OSAGE SMALL LETTER EHCHA
					break;
				case 0x104B7:		//	𐒷	0xf0 0x90 0x92 0xb7	OSAGE CAPITAL LETTER E
					*p = 0x104DF;	//	𐓟	0xf0 0x90 0x93 0x9f	OSAGE SMALL LETTER E
					break;
				case 0x104B8:		//	𐒸	0xf0 0x90 0x92 0xb8	OSAGE CAPITAL LETTER EIN
					*p = 0x104E0;	//	𐓠	0xf0 0x90 0x93 0xa0	OSAGE SMALL LETTER EIN
					break;
				case 0x104B9:		//	𐒹	0xf0 0x90 0x92 0xb9	OSAGE CAPITAL LETTER HA
					*p = 0x104E1;	//	𐓡	0xf0 0x90 0x93 0xa1	OSAGE SMALL LETTER HA
					break;
				case 0x104BA:		//	𐒺	0xf0 0x90 0x92 0xba	OSAGE CAPITAL LETTER HYA
					*p = 0x104E2;	//	𐓢	0xf0 0x90 0x93 0xa2	OSAGE SMALL LETTER HYA
					break;
				case 0x104BB:		//	𐒻	0xf0 0x90 0x92 0xbb	OSAGE CAPITAL LETTER I
					*p = 0x104E3;	//	𐓣	0xf0 0x90 0x93 0xa3	OSAGE SMALL LETTER I
					break;
				case 0x104BC:		//	𐒼	0xf0 0x90 0x92 0xbc	OSAGE CAPITAL LETTER KA
					*p = 0x104E4;	//	𐓤	0xf0 0x90 0x93 0xa4	OSAGE SMALL LETTER KA
					break;
				case 0x104BD:		//	𐒽	0xf0 0x90 0x92 0xbd	OSAGE CAPITAL LETTER EHKA
					*p = 0x104E5;	//	𐓥	0xf0 0x90 0x93 0xa5	OSAGE SMALL LETTER EHKA
					break;
				case 0x104BE:		//	𐒾	0xf0 0x90 0x92 0xbe	OSAGE CAPITAL LETTER KYA
					*p = 0x104E6;	//	𐓦	0xf0 0x90 0x93 0xa6	OSAGE SMALL LETTER KYA
					break;
				case 0x104BF:		//	𐒿	0xf0 0x90 0x92 0xbf	OSAGE CAPITAL LETTER LA
					*p = 0x104E7;	//	𐓧	0xf0 0x90 0x93 0xa7	OSAGE SMALL LETTER LA
					break;
				case 0x104C0:		//	𐓀	0xf0 0x90 0x93 0x80	OSAGE CAPITAL LETTER MA
					*p = 0x104E8;	//	𐓨	0xf0 0x90 0x93 0xa8	OSAGE SMALL LETTER MA
					break;
				case 0x104C1:		//	𐓁	0xf0 0x90 0x93 0x81	OSAGE CAPITAL LETTER NA
					*p = 0x104E9;	//	𐓩	0xf0 0x90 0x93 0xa9	OSAGE SMALL LETTER NA
					break;
				case 0x104C2:		//	𐓂	0xf0 0x90 0x93 0x82	OSAGE CAPITAL LETTER O
					*p = 0x104EA;	//	𐓪	0xf0 0x90 0x93 0xaa	OSAGE SMALL LETTER O
					break;
				case 0x104C3:		//	𐓃	0xf0 0x90 0x93 0x83	OSAGE CAPITAL LETTER OIN
					*p = 0x104EB;	//	𐓫	0xf0 0x90 0x93 0xab	OSAGE SMALL LETTER OIN
					break;
				case 0x104C4:		//	𐓄	0xf0 0x90 0x93 0x84	OSAGE CAPITAL LETTER PA
					*p = 0x104EC;	//	𐓬	0xf0 0x90 0x93 0xac	OSAGE SMALL LETTER PA
					break;
				case 0x104C5:		//	𐓅	0xf0 0x90 0x93 0x85	OSAGE CAPITAL LETTER EHPA
					*p = 0x104ED;	//	𐓭	0xf0 0x90 0x93 0xad	OSAGE SMALL LETTER EHPA
					break;
				case 0x104C6:		//	𐓆	0xf0 0x90 0x93 0x86	OSAGE CAPITAL LETTER SA
					*p = 0x104EE;	//	𐓮	0xf0 0x90 0x93 0xae	OSAGE SMALL LETTER SA
					break;
				case 0x104C7:		//	𐓇	0xf0 0x90 0x93 0x87	OSAGE CAPITAL LETTER SHA
					*p = 0x104EF;	//	𐓯	0xf0 0x90 0x93 0xaf	OSAGE SMALL LETTER SHA
					break;
				case 0x104C8:		//	𐓈	0xf0 0x90 0x93 0x88	OSAGE CAPITAL LETTER TA
					*p = 0x104F0;	//	𐓰	0xf0 0x90 0x93 0xb0	OSAGE SMALL LETTER TA
					break;
				case 0x104C9:		//	𐓉	0xf0 0x90 0x93 0x89	OSAGE CAPITAL LETTER EHTA
					*p = 0x104F1;	//	𐓱	0xf0 0x90 0x93 0xb1	OSAGE SMALL LETTER EHTA
					break;
				case 0x104CA:		//	𐓊	0xf0 0x90 0x93 0x8a	OSAGE CAPITAL LETTER TSA
					*p = 0x104F2;	//	𐓲	0xf0 0x90 0x93 0xb2	OSAGE SMALL LETTER TSA
					break;
				case 0x104CB:		//	𐓋	0xf0 0x90 0x93 0x8b	OSAGE CAPITAL LETTER EHTSA
					*p = 0x104F3;	//	𐓳	0xf0 0x90 0x93 0xb3	OSAGE SMALL LETTER EHTSA
					break;
				case 0x104CC:		//	𐓌	0xf0 0x90 0x93 0x8c	OSAGE CAPITAL LETTER TSHA
					*p = 0x104F4;	//	𐓴	0xf0 0x90 0x93 0xb4	OSAGE SMALL LETTER TSHA
					break;
				case 0x104CD:		//	𐓍	0xf0 0x90 0x93 0x8d	OSAGE CAPITAL LETTER DHA
					*p = 0x104F5;	//	𐓵	0xf0 0x90 0x93 0xb5	OSAGE SMALL LETTER DHA
					break;
				case 0x104CE:		//	𐓎	0xf0 0x90 0x93 0x8e	OSAGE CAPITAL LETTER U
					*p = 0x104F6;	//	𐓶	0xf0 0x90 0x93 0xb6	OSAGE SMALL LETTER U
					break;
				case 0x104CF:		//	𐓏	0xf0 0x90 0x93 0x8f	OSAGE CAPITAL LETTER WA
					*p = 0x104F7;	//	𐓷	0xf0 0x90 0x93 0xb7	OSAGE SMALL LETTER WA
					break;
				case 0x104D0:		//	𐓐	0xf0 0x90 0x93 0x90	OSAGE CAPITAL LETTER KHA
					*p = 0x104F8;	//	𐓸	0xf0 0x90 0x93 0xb8	OSAGE SMALL LETTER KHA
					break;
				case 0x104D1:		//	𐓑	0xf0 0x90 0x93 0x91	OSAGE CAPITAL LETTER GHA
					*p = 0x104F9;	//	𐓹	0xf0 0x90 0x93 0xb9	OSAGE SMALL LETTER GHA
					break;
				case 0x104D2:		//	𐓒	0xf0 0x90 0x93 0x92	OSAGE CAPITAL LETTER ZA
					*p = 0x104FA;	//	𐓺	0xf0 0x90 0x93 0xba	OSAGE SMALL LETTER ZA
					break;
				case 0x104D3:		//	𐓓	0xf0 0x90 0x93 0x93	OSAGE CAPITAL LETTER ZHA
					*p = 0x104FB;	//	𐓻	0xf0 0x90 0x93 0xbb	OSAGE SMALL LETTER ZHA
					break;
				case 0x10C80:		//	𐲀	0xf0 0x90 0xb2 0x80	OLD HUNGARIAN CAPITAL LETTER A
					*p = 0x10CC0;	//	𐳀	0xf0 0x90 0xb3 0x80	OLD HUNGARIAN SMALL LETTER A
					break;
				case 0x10C81:		//	𐲁	0xf0 0x90 0xb2 0x81	OLD HUNGARIAN CAPITAL LETTER AA
					*p = 0x10CC1;	//	𐳁	0xf0 0x90 0xb3 0x81	OLD HUNGARIAN SMALL LETTER AA
					break;
				case 0x10C82:		//	𐲂	0xf0 0x90 0xb2 0x82	OLD HUNGARIAN CAPITAL LETTER EB
					*p = 0x10CC2;	//	𐳂	0xf0 0x90 0xb3 0x82	OLD HUNGARIAN SMALL LETTER EB
					break;
				case 0x10C83:		//	𐲃	0xf0 0x90 0xb2 0x83	OLD HUNGARIAN CAPITAL LETTER AMB
					*p = 0x10CC3;	//	𐳃	0xf0 0x90 0xb3 0x83	OLD HUNGARIAN SMALL LETTER AMB
					break;
				case 0x10C84:		//	𐲄	0xf0 0x90 0xb2 0x84	OLD HUNGARIAN CAPITAL LETTER EC
					*p = 0x10CC4;	//	𐳄	0xf0 0x90 0xb3 0x84	OLD HUNGARIAN SMALL LETTER EC
					break;
				case 0x10C85:		//	𐲅	0xf0 0x90 0xb2 0x85	OLD HUNGARIAN CAPITAL LETTER ENC
					*p = 0x10CC5;	//	𐳅	0xf0 0x90 0xb3 0x85	OLD HUNGARIAN SMALL LETTER ENC
					break;
				case 0x10C86:		//	𐲆	0xf0 0x90 0xb2 0x86	OLD HUNGARIAN CAPITAL LETTER ECS
					*p = 0x10CC6;	//	𐳆	0xf0 0x90 0xb3 0x86	OLD HUNGARIAN SMALL LETTER ECS
					break;
				case 0x10C87:		//	𐲇	0xf0 0x90 0xb2 0x87	OLD HUNGARIAN CAPITAL LETTER ED
					*p = 0x10CC7;	//	𐳇	0xf0 0x90 0xb3 0x87	OLD HUNGARIAN SMALL LETTER ED
					break;
				case 0x10C88:		//	𐲈	0xf0 0x90 0xb2 0x88	OLD HUNGARIAN CAPITAL LETTER AND
					*p = 0x10CC8;	//	𐳈	0xf0 0x90 0xb3 0x88	OLD HUNGARIAN SMALL LETTER AND
					break;
				case 0x10C89:		//	𐲉	0xf0 0x90 0xb2 0x89	OLD HUNGARIAN CAPITAL LETTER E
					*p = 0x10CC9;	//	𐳉	0xf0 0x90 0xb3 0x89	OLD HUNGARIAN SMALL LETTER E
					break;
				case 0x10C8A:		//	𐲊	0xf0 0x90 0xb2 0x8a	OLD HUNGARIAN CAPITAL LETTER CLOSE E
					*p = 0x10CCA;	//	𐳊	0xf0 0x90 0xb3 0x8a	OLD HUNGARIAN SMALL LETTER CLOSE E
					break;
				case 0x10C8B:		//	𐲋	0xf0 0x90 0xb2 0x8b	OLD HUNGARIAN CAPITAL LETTER EE
					*p = 0x10CCB;	//	𐳋	0xf0 0x90 0xb3 0x8b	OLD HUNGARIAN SMALL LETTER EE
					break;
				case 0x10C8C:		//	𐲌	0xf0 0x90 0xb2 0x8c	OLD HUNGARIAN CAPITAL LETTER EF
					*p = 0x10CCC;	//	𐳌	0xf0 0x90 0xb3 0x8c	OLD HUNGARIAN SMALL LETTER EF
					break;
				case 0x10C8D:		//	𐲍	0xf0 0x90 0xb2 0x8d	OLD HUNGARIAN CAPITAL LETTER EG
					*p = 0x10CCD;	//	𐳍	0xf0 0x90 0xb3 0x8d	OLD HUNGARIAN SMALL LETTER EG
					break;
				case 0x10C8E:		//	𐲎	0xf0 0x90 0xb2 0x8e	OLD HUNGARIAN CAPITAL LETTER EGY
					*p = 0x10CCE;	//	𐳎	0xf0 0x90 0xb3 0x8e	OLD HUNGARIAN SMALL LETTER EGY
					break;
				case 0x10C8F:		//	𐲏	0xf0 0x90 0xb2 0x8f	OLD HUNGARIAN CAPITAL LETTER EH
					*p = 0x10CCF;	//	𐳏	0xf0 0x90 0xb3 0x8f	OLD HUNGARIAN SMALL LETTER EH
					break;
				case 0x10C90:		//	𐲐	0xf0 0x90 0xb2 0x90	OLD HUNGARIAN CAPITAL LETTER I
					*p = 0x10CD0;	//	𐳐	0xf0 0x90 0xb3 0x90	OLD HUNGARIAN SMALL LETTER I
					break;
				case 0x10C91:		//	𐲑	0xf0 0x90 0xb2 0x91	OLD HUNGARIAN CAPITAL LETTER II
					*p = 0x10CD1;	//	𐳑	0xf0 0x90 0xb3 0x91	OLD HUNGARIAN SMALL LETTER II
					break;
				case 0x10C92:		//	𐲒	0xf0 0x90 0xb2 0x92	OLD HUNGARIAN CAPITAL LETTER EJ
					*p = 0x10CD2;	//	𐳒	0xf0 0x90 0xb3 0x92	OLD HUNGARIAN SMALL LETTER EJ
					break;
				case 0x10C93:		//	𐲓	0xf0 0x90 0xb2 0x93	OLD HUNGARIAN CAPITAL LETTER EK
					*p = 0x10CD3;	//	𐳓	0xf0 0x90 0xb3 0x93	OLD HUNGARIAN SMALL LETTER EK
					break;
				case 0x10C94:		//	𐲔	0xf0 0x90 0xb2 0x94	OLD HUNGARIAN CAPITAL LETTER AK
					*p = 0x10CD4;	//	𐳔	0xf0 0x90 0xb3 0x94	OLD HUNGARIAN SMALL LETTER AK
					break;
				case 0x10C95:		//	𐲕	0xf0 0x90 0xb2 0x95	OLD HUNGARIAN CAPITAL LETTER UNK
					*p = 0x10CD5;	//	𐳕	0xf0 0x90 0xb3 0x95	OLD HUNGARIAN SMALL LETTER UNK
					break;
				case 0x10C96:		//	𐲖	0xf0 0x90 0xb2 0x96	OLD HUNGARIAN CAPITAL LETTER EL
					*p = 0x10CD6;	//	𐳖	0xf0 0x90 0xb3 0x96	OLD HUNGARIAN SMALL LETTER EL
					break;
				case 0x10C97:		//	𐲗	0xf0 0x90 0xb2 0x97	OLD HUNGARIAN CAPITAL LETTER ELY
					*p = 0x10CD7;	//	𐳗	0xf0 0x90 0xb3 0x97	OLD HUNGARIAN SMALL LETTER ELY
					break;
				case 0x10C98:		//	𐲘	0xf0 0x90 0xb2 0x98	OLD HUNGARIAN CAPITAL LETTER EM
					*p = 0x10CD8;	//	𐳘	0xf0 0x90 0xb3 0x98	OLD HUNGARIAN SMALL LETTER EM
					break;
				case 0x10C99:		//	𐲙	0xf0 0x90 0xb2 0x99	OLD HUNGARIAN CAPITAL LETTER EN
					*p = 0x10CD9;	//	𐳙	0xf0 0x90 0xb3 0x99	OLD HUNGARIAN SMALL LETTER EN
					break;
				case 0x10C9A:		//	𐲚	0xf0 0x90 0xb2 0x9a	OLD HUNGARIAN CAPITAL LETTER ENY
					*p = 0x10CDA;	//	𐳚	0xf0 0x90 0xb3 0x9a	OLD HUNGARIAN SMALL LETTER ENY
					break;
				case 0x10C9B:		//	𐲛	0xf0 0x90 0xb2 0x9b	OLD HUNGARIAN CAPITAL LETTER O
					*p = 0x10CDB;	//	𐳛	0xf0 0x90 0xb3 0x9b	OLD HUNGARIAN SMALL LETTER O
					break;
				case 0x10C9C:		//	𐲜	0xf0 0x90 0xb2 0x9c	OLD HUNGARIAN CAPITAL LETTER OO
					*p = 0x10CDC;	//	𐳜	0xf0 0x90 0xb3 0x9c	OLD HUNGARIAN SMALL LETTER OO
					break;
				case 0x10C9D:		//	𐲝	0xf0 0x90 0xb2 0x9d	OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG OE
					*p = 0x10CDD;	//	𐳝	0xf0 0x90 0xb3 0x9d	OLD HUNGARIAN SMALL LETTER NIKOLSBURG OE
					break;
				case 0x10C9E:		//	𐲞	0xf0 0x90 0xb2 0x9e	OLD HUNGARIAN CAPITAL LETTER RUDIMENTA OE
					*p = 0x10CDE;	//	𐳞	0xf0 0x90 0xb3 0x9e	OLD HUNGARIAN SMALL LETTER RUDIMENTA OE
					break;
				case 0x10C9F:		//	𐲟	0xf0 0x90 0xb2 0x9f	OLD HUNGARIAN CAPITAL LETTER OEE
					*p = 0x10CDF;	//	𐳟	0xf0 0x90 0xb3 0x9f	OLD HUNGARIAN SMALL LETTER OEE
					break;
				case 0x10CA0:		//	𐲠	0xf0 0x90 0xb2 0xa0	OLD HUNGARIAN CAPITAL LETTER EP
					*p = 0x10CE0;	//	𐳠	0xf0 0x90 0xb3 0xa0	OLD HUNGARIAN SMALL LETTER EP
					break;
				case 0x10CA1:		//	𐲡	0xf0 0x90 0xb2 0xa1	OLD HUNGARIAN CAPITAL LETTER EMP
					*p = 0x10CE1;	//	𐳡	0xf0 0x90 0xb3 0xa1	OLD HUNGARIAN SMALL LETTER EMP
					break;
				case 0x10CA2:		//	𐲢	0xf0 0x90 0xb2 0xa2	OLD HUNGARIAN CAPITAL LETTER ER
					*p = 0x10CE2;	//	𐳢	0xf0 0x90 0xb3 0xa2	OLD HUNGARIAN SMALL LETTER ER
					break;
				case 0x10CA3:		//	𐲣	0xf0 0x90 0xb2 0xa3	OLD HUNGARIAN CAPITAL LETTER SHORT ER
					*p = 0x10CE3;	//	𐳣	0xf0 0x90 0xb3 0xa3	OLD HUNGARIAN SMALL LETTER SHORT ER
					break;
				case 0x10CA4:		//	𐲤	0xf0 0x90 0xb2 0xa4	OLD HUNGARIAN CAPITAL LETTER ES
					*p = 0x10CE4;	//	𐳤	0xf0 0x90 0xb3 0xa4	OLD HUNGARIAN SMALL LETTER ES
					break;
				case 0x10CA5:		//	𐲥	0xf0 0x90 0xb2 0xa5	OLD HUNGARIAN CAPITAL LETTER ESZ
					*p = 0x10CE5;	//	𐳥	0xf0 0x90 0xb3 0xa5	OLD HUNGARIAN SMALL LETTER ESZ
					break;
				case 0x10CA6:		//	𐲦	0xf0 0x90 0xb2 0xa6	OLD HUNGARIAN CAPITAL LETTER ET
					*p = 0x10CE6;	//	𐳦	0xf0 0x90 0xb3 0xa6	OLD HUNGARIAN SMALL LETTER ET
					break;
				case 0x10CA7:		//	𐲧	0xf0 0x90 0xb2 0xa7	OLD HUNGARIAN CAPITAL LETTER ENT
					*p = 0x10CE7;	//	𐳧	0xf0 0x90 0xb3 0xa7	OLD HUNGARIAN SMALL LETTER ENT
					break;
				case 0x10CA8:		//	𐲨	0xf0 0x90 0xb2 0xa8	OLD HUNGARIAN CAPITAL LETTER ETY
					*p = 0x10CE8;	//	𐳨	0xf0 0x90 0xb3 0xa8	OLD HUNGARIAN SMALL LETTER ETY
					break;
				case 0x10CA9:		//	𐲩	0xf0 0x90 0xb2 0xa9	OLD HUNGARIAN CAPITAL LETTER ECH
					*p = 0x10CE9;	//	𐳩	0xf0 0x90 0xb3 0xa9	OLD HUNGARIAN SMALL LETTER ECH
					break;
				case 0x10CAA:		//	𐲪	0xf0 0x90 0xb2 0xaa	OLD HUNGARIAN CAPITAL LETTER U
					*p = 0x10CEA;	//	𐳪	0xf0 0x90 0xb3 0xaa	OLD HUNGARIAN SMALL LETTER U
					break;
				case 0x10CAB:		//	𐲫	0xf0 0x90 0xb2 0xab	OLD HUNGARIAN CAPITAL LETTER UU
					*p = 0x10CEB;	//	𐳫	0xf0 0x90 0xb3 0xab	OLD HUNGARIAN SMALL LETTER UU
					break;
				case 0x10CAC:		//	𐲬	0xf0 0x90 0xb2 0xac	OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG UE
					*p = 0x10CEC;	//	𐳬	0xf0 0x90 0xb3 0xac	OLD HUNGARIAN SMALL LETTER NIKOLSBURG UE
					break;
				case 0x10CAD:		//	𐲭	0xf0 0x90 0xb2 0xad	OLD HUNGARIAN CAPITAL LETTER RUDIMENTA UE
					*p = 0x10CED;	//	𐳭	0xf0 0x90 0xb3 0xad	OLD HUNGARIAN SMALL LETTER RUDIMENTA UE
					break;
				case 0x10CAE:		//	𐲮	0xf0 0x90 0xb2 0xae	OLD HUNGARIAN CAPITAL LETTER EV
					*p = 0x10CEE;	//	𐳮	0xf0 0x90 0xb3 0xae	OLD HUNGARIAN SMALL LETTER EV
					break;
				case 0x10CAF:		//	𐲯	0xf0 0x90 0xb2 0xaf	OLD HUNGARIAN CAPITAL LETTER EZ
					*p = 0x10CEF;	//	𐳯	0xf0 0x90 0xb3 0xaf	OLD HUNGARIAN SMALL LETTER EZ
					break;
				case 0x10CB0:		//	𐲰	0xf0 0x90 0xb2 0xb0	OLD HUNGARIAN CAPITAL LETTER EZS
					*p = 0x10CF0;	//	𐳰	0xf0 0x90 0xb3 0xb0	OLD HUNGARIAN SMALL LETTER EZS
					break;
				case 0x10CB1:		//	𐲱	0xf0 0x90 0xb2 0xb1	OLD HUNGARIAN CAPITAL LETTER ENT-SHAPED SIGN
					*p = 0x10CF1;	//	𐳱	0xf0 0x90 0xb3 0xb1	OLD HUNGARIAN SMALL LETTER ENT-SHAPED SIGN
					break;
				case 0x10CB2:		//	𐲲	0xf0 0x90 0xb2 0xb2	OLD HUNGARIAN CAPITAL LETTER US
					*p = 0x10CF2;	//	𐳲	0xf0 0x90 0xb3 0xb2	OLD HUNGARIAN SMALL LETTER US
					break;
				case 0x118A0:		//	𑢠	0xf0 0x91 0xa2 0xa0	WARANG CITI CAPITAL LETTER NGAA
					*p = 0x118C0;	//	𑣀	0xf0 0x91 0xa3 0x80	WARANG CITI SMALL LETTER NGAA
					break;
				case 0x118A1:		//	𑢡	0xf0 0x91 0xa2 0xa1	WARANG CITI CAPITAL LETTER A
					*p = 0x118C1;	//	𑣁	0xf0 0x91 0xa3 0x81	WARANG CITI SMALL LETTER A
					break;
				case 0x118A2:		//	𑢢	0xf0 0x91 0xa2 0xa2	WARANG CITI CAPITAL LETTER WI
					*p = 0x118C2;	//	𑣂	0xf0 0x91 0xa3 0x82	WARANG CITI SMALL LETTER WI
					break;
				case 0x118A3:		//	𑢣	0xf0 0x91 0xa2 0xa3	WARANG CITI CAPITAL LETTER YU
					*p = 0x118C3;	//	𑣃	0xf0 0x91 0xa3 0x83	WARANG CITI SMALL LETTER YU
					break;
				case 0x118A4:		//	𑢤	0xf0 0x91 0xa2 0xa4	WARANG CITI CAPITAL LETTER YA
					*p = 0x118C4;	//	𑣄	0xf0 0x91 0xa3 0x84	WARANG CITI SMALL LETTER YA
					break;
				case 0x118A5:		//	𑢥	0xf0 0x91 0xa2 0xa5	WARANG CITI CAPITAL LETTER YO
					*p = 0x118C5;	//	𑣅	0xf0 0x91 0xa3 0x85	WARANG CITI SMALL LETTER YO
					break;
				case 0x118A6:		//	𑢦	0xf0 0x91 0xa2 0xa6	WARANG CITI CAPITAL LETTER II
					*p = 0x118C6;	//	𑣆	0xf0 0x91 0xa3 0x86	WARANG CITI SMALL LETTER II
					break;
				case 0x118A7:		//	𑢧	0xf0 0x91 0xa2 0xa7	WARANG CITI CAPITAL LETTER UU
					*p = 0x118C7;	//	𑣇	0xf0 0x91 0xa3 0x87	WARANG CITI SMALL LETTER UU
					break;
				case 0x118A8:		//	𑢨	0xf0 0x91 0xa2 0xa8	WARANG CITI CAPITAL LETTER E
					*p = 0x118C8;	//	𑣈	0xf0 0x91 0xa3 0x88	WARANG CITI SMALL LETTER E
					break;
				case 0x118A9:		//	𑢩	0xf0 0x91 0xa2 0xa9	WARANG CITI CAPITAL LETTER O
					*p = 0x118C9;	//	𑣉	0xf0 0x91 0xa3 0x89	WARANG CITI SMALL LETTER O
					break;
				case 0x118AA:		//	𑢪	0xf0 0x91 0xa2 0xaa	WARANG CITI CAPITAL LETTER ANG
					*p = 0x118CA;	//	𑣊	0xf0 0x91 0xa3 0x8a	WARANG CITI SMALL LETTER ANG
					break;
				case 0x118AB:		//	𑢫	0xf0 0x91 0xa2 0xab	WARANG CITI CAPITAL LETTER GA
					*p = 0x118CB;	//	𑣋	0xf0 0x91 0xa3 0x8b	WARANG CITI SMALL LETTER GA
					break;
				case 0x118AC:		//	𑢬	0xf0 0x91 0xa2 0xac	WARANG CITI CAPITAL LETTER KO
					*p = 0x118CC;	//	𑣌	0xf0 0x91 0xa3 0x8c	WARANG CITI SMALL LETTER KO
					break;
				case 0x118AD:		//	𑢭	0xf0 0x91 0xa2 0xad	WARANG CITI CAPITAL LETTER ENY
					*p = 0x118CD;	//	𑣍	0xf0 0x91 0xa3 0x8d	WARANG CITI SMALL LETTER ENY
					break;
				case 0x118AE:		//	𑢮	0xf0 0x91 0xa2 0xae	WARANG CITI CAPITAL LETTER YUJ
					*p = 0x118CE;	//	𑣎	0xf0 0x91 0xa3 0x8e	WARANG CITI SMALL LETTER YUJ
					break;
				case 0x118AF:		//	𑢯	0xf0 0x91 0xa2 0xaf	WARANG CITI CAPITAL LETTER UC
					*p = 0x118CF;	//	𑣏	0xf0 0x91 0xa3 0x8f	WARANG CITI SMALL LETTER UC
					break;
				case 0x118B0:		//	𑢰	0xf0 0x91 0xa2 0xb0	WARANG CITI CAPITAL LETTER ENN
					*p = 0x118D0;	//	𑣐	0xf0 0x91 0xa3 0x90	WARANG CITI SMALL LETTER ENN
					break;
				case 0x118B1:		//	𑢱	0xf0 0x91 0xa2 0xb1	WARANG CITI CAPITAL LETTER ODD
					*p = 0x118D1;	//	𑣑	0xf0 0x91 0xa3 0x91	WARANG CITI SMALL LETTER ODD
					break;
				case 0x118B2:		//	𑢲	0xf0 0x91 0xa2 0xb2	WARANG CITI CAPITAL LETTER TTE
					*p = 0x118D2;	//	𑣒	0xf0 0x91 0xa3 0x92	WARANG CITI SMALL LETTER TTE
					break;
				case 0x118B3:		//	𑢳	0xf0 0x91 0xa2 0xb3	WARANG CITI CAPITAL LETTER NUNG
					*p = 0x118D3;	//	𑣓	0xf0 0x91 0xa3 0x93	WARANG CITI SMALL LETTER NUNG
					break;
				case 0x118B4:		//	𑢴	0xf0 0x91 0xa2 0xb4	WARANG CITI CAPITAL LETTER DA
					*p = 0x118D4;	//	𑣔	0xf0 0x91 0xa3 0x94	WARANG CITI SMALL LETTER DA
					break;
				case 0x118B5:		//	𑢵	0xf0 0x91 0xa2 0xb5	WARANG CITI CAPITAL LETTER AT
					*p = 0x118D5;	//	𑣕	0xf0 0x91 0xa3 0x95	WARANG CITI SMALL LETTER AT
					break;
				case 0x118B6:		//	𑢶	0xf0 0x91 0xa2 0xb6	WARANG CITI CAPITAL LETTER AM
					*p = 0x118D6;	//	𑣖	0xf0 0x91 0xa3 0x96	WARANG CITI SMALL LETTER AM
					break;
				case 0x118B7:		//	𑢷	0xf0 0x91 0xa2 0xb7	WARANG CITI CAPITAL LETTER BU
					*p = 0x118D7;	//	𑣗	0xf0 0x91 0xa3 0x97	WARANG CITI SMALL LETTER BU
					break;
				case 0x118B8:		//	𑢸	0xf0 0x91 0xa2 0xb8	WARANG CITI CAPITAL LETTER PU
					*p = 0x118D8;	//	𑣘	0xf0 0x91 0xa3 0x98	WARANG CITI SMALL LETTER PU
					break;
				case 0x118B9:		//	𑢹	0xf0 0x91 0xa2 0xb9	WARANG CITI CAPITAL LETTER HIYO
					*p = 0x118D9;	//	𑣙	0xf0 0x91 0xa3 0x99	WARANG CITI SMALL LETTER HIYO
					break;
				case 0x118BA:		//	𑢺	0xf0 0x91 0xa2 0xba	WARANG CITI CAPITAL LETTER HOLO
					*p = 0x118DA;	//	𑣚	0xf0 0x91 0xa3 0x9a	WARANG CITI SMALL LETTER HOLO
					break;
				case 0x118BB:		//	𑢻	0xf0 0x91 0xa2 0xbb	WARANG CITI CAPITAL LETTER HORR
					*p = 0x118DB;	//	𑣛	0xf0 0x91 0xa3 0x9b	WARANG CITI SMALL LETTER HORR
					break;
				case 0x118BC:		//	𑢼	0xf0 0x91 0xa2 0xbc	WARANG CITI CAPITAL LETTER HAR
					*p = 0x118DC;	//	𑣜	0xf0 0x91 0xa3 0x9c	WARANG CITI SMALL LETTER HAR
					break;
				case 0x118BD:		//	𑢽	0xf0 0x91 0xa2 0xbd	WARANG CITI CAPITAL LETTER SSUU
					*p = 0x118DD;	//	𑣝	0xf0 0x91 0xa3 0x9d	WARANG CITI SMALL LETTER SSUU
					break;
				case 0x118BE:		//	𑢾	0xf0 0x91 0xa2 0xbe	WARANG CITI CAPITAL LETTER SII
					*p = 0x118DE;	//	𑣞	0xf0 0x91 0xa3 0x9e	WARANG CITI SMALL LETTER SII
					break;
				case 0x118BF:		//	𑢿	0xf0 0x91 0xa2 0xbf	WARANG CITI CAPITAL LETTER VIYO
					*p = 0x118DF;	//	𑣟	0xf0 0x91 0xa3 0x9f	WARANG CITI SMALL LETTER VIYO
					break;
				case 0x16E40:		//	𖹀	0xf0 0x96 0xb9 0x80	MEDEFAIDRIN CAPITAL LETTER M
					*p = 0x16E60;	//	𖹠	0xf0 0x96 0xb9 0xa0	MEDEFAIDRIN SMALL LETTER M
					break;
				case 0x16E41:		//	𖹁	0xf0 0x96 0xb9 0x81	MEDEFAIDRIN CAPITAL LETTER S
					*p = 0x16E61;	//	𖹡	0xf0 0x96 0xb9 0xa1	MEDEFAIDRIN SMALL LETTER S
					break;
				case 0x16E42:		//	𖹂	0xf0 0x96 0xb9 0x82	MEDEFAIDRIN CAPITAL LETTER V
					*p = 0x16E62;	//	𖹢	0xf0 0x96 0xb9 0xa2	MEDEFAIDRIN SMALL LETTER V
					break;
				case 0x16E43:		//	𖹃	0xf0 0x96 0xb9 0x83	MEDEFAIDRIN CAPITAL LETTER W
					*p = 0x16E63;	//	𖹣	0xf0 0x96 0xb9 0xa3	MEDEFAIDRIN SMALL LETTER W
					break;
				case 0x16E44:		//	𖹄	0xf0 0x96 0xb9 0x84	MEDEFAIDRIN CAPITAL LETTER ATIU
					*p = 0x16E64;	//	𖹤	0xf0 0x96 0xb9 0xa4	MEDEFAIDRIN SMALL LETTER ATIU
					break;
				case 0x16E45:		//	𖹅	0xf0 0x96 0xb9 0x85	MEDEFAIDRIN CAPITAL LETTER Z
					*p = 0x16E65;	//	𖹥	0xf0 0x96 0xb9 0xa5	MEDEFAIDRIN SMALL LETTER Z
					break;
				case 0x16E46:		//	𖹆	0xf0 0x96 0xb9 0x86	MEDEFAIDRIN CAPITAL LETTER KP
					*p = 0x16E66;	//	𖹦	0xf0 0x96 0xb9 0xa6	MEDEFAIDRIN SMALL LETTER KP
					break;
				case 0x16E47:		//	𖹇	0xf0 0x96 0xb9 0x87	MEDEFAIDRIN CAPITAL LETTER P
					*p = 0x16E67;	//	𖹧	0xf0 0x96 0xb9 0xa7	MEDEFAIDRIN SMALL LETTER P
					break;
				case 0x16E48:		//	𖹈	0xf0 0x96 0xb9 0x88	MEDEFAIDRIN CAPITAL LETTER T
					*p = 0x16E68;	//	𖹨	0xf0 0x96 0xb9 0xa8	MEDEFAIDRIN SMALL LETTER T
					break;
				case 0x16E49:		//	𖹉	0xf0 0x96 0xb9 0x89	MEDEFAIDRIN CAPITAL LETTER G
					*p = 0x16E69;	//	𖹩	0xf0 0x96 0xb9 0xa9	MEDEFAIDRIN SMALL LETTER G
					break;
				case 0x16E4A:		//	𖹊	0xf0 0x96 0xb9 0x8a	MEDEFAIDRIN CAPITAL LETTER F
					*p = 0x16E6A;	//	𖹪	0xf0 0x96 0xb9 0xaa	MEDEFAIDRIN SMALL LETTER F
					break;
				case 0x16E4B:		//	𖹋	0xf0 0x96 0xb9 0x8b	MEDEFAIDRIN CAPITAL LETTER I
					*p = 0x16E6B;	//	𖹫	0xf0 0x96 0xb9 0xab	MEDEFAIDRIN SMALL LETTER I
					break;
				case 0x16E4C:		//	𖹌	0xf0 0x96 0xb9 0x8c	MEDEFAIDRIN CAPITAL LETTER K
					*p = 0x16E6C;	//	𖹬	0xf0 0x96 0xb9 0xac	MEDEFAIDRIN SMALL LETTER K
					break;
				case 0x16E4D:		//	𖹍	0xf0 0x96 0xb9 0x8d	MEDEFAIDRIN CAPITAL LETTER A
					*p = 0x16E6D;	//	𖹭	0xf0 0x96 0xb9 0xad	MEDEFAIDRIN SMALL LETTER A
					break;
				case 0x16E4E:		//	𖹎	0xf0 0x96 0xb9 0x8e	MEDEFAIDRIN CAPITAL LETTER J
					*p = 0x16E6E;	//	𖹮	0xf0 0x96 0xb9 0xae	MEDEFAIDRIN SMALL LETTER J
					break;
				case 0x16E4F:		//	𖹏	0xf0 0x96 0xb9 0x8f	MEDEFAIDRIN CAPITAL LETTER E
					*p = 0x16E6F;	//	𖹯	0xf0 0x96 0xb9 0xaf	MEDEFAIDRIN SMALL LETTER E
					break;
				case 0x16E50:		//	𖹐	0xf0 0x96 0xb9 0x90	MEDEFAIDRIN CAPITAL LETTER B
					*p = 0x16E70;	//	𖹰	0xf0 0x96 0xb9 0xb0	MEDEFAIDRIN SMALL LETTER B
					break;
				case 0x16E51:		//	𖹑	0xf0 0x96 0xb9 0x91	MEDEFAIDRIN CAPITAL LETTER C
					*p = 0x16E71;	//	𖹱	0xf0 0x96 0xb9 0xb1	MEDEFAIDRIN SMALL LETTER C
					break;
				case 0x16E52:		//	𖹒	0xf0 0x96 0xb9 0x92	MEDEFAIDRIN CAPITAL LETTER U
					*p = 0x16E72;	//	𖹲	0xf0 0x96 0xb9 0xb2	MEDEFAIDRIN SMALL LETTER U
					break;
				case 0x16E53:		//	𖹓	0xf0 0x96 0xb9 0x93	MEDEFAIDRIN CAPITAL LETTER YU
					*p = 0x16E73;	//	𖹳	0xf0 0x96 0xb9 0xb3	MEDEFAIDRIN SMALL LETTER YU
					break;
				case 0x16E54:		//	𖹔	0xf0 0x96 0xb9 0x94	MEDEFAIDRIN CAPITAL LETTER L
					*p = 0x16E74;	//	𖹴	0xf0 0x96 0xb9 0xb4	MEDEFAIDRIN SMALL LETTER L
					break;
				case 0x16E55:		//	𖹕	0xf0 0x96 0xb9 0x95	MEDEFAIDRIN CAPITAL LETTER Q
					*p = 0x16E75;	//	𖹵	0xf0 0x96 0xb9 0xb5	MEDEFAIDRIN SMALL LETTER Q
					break;
				case 0x16E56:		//	𖹖	0xf0 0x96 0xb9 0x96	MEDEFAIDRIN CAPITAL LETTER HP
					*p = 0x16E76;	//	𖹶	0xf0 0x96 0xb9 0xb6	MEDEFAIDRIN SMALL LETTER HP
					break;
				case 0x16E57:		//	𖹗	0xf0 0x96 0xb9 0x97	MEDEFAIDRIN CAPITAL LETTER NY
					*p = 0x16E77;	//	𖹷	0xf0 0x96 0xb9 0xb7	MEDEFAIDRIN SMALL LETTER NY
					break;
				case 0x16E58:		//	𖹘	0xf0 0x96 0xb9 0x98	MEDEFAIDRIN CAPITAL LETTER X
					*p = 0x16E78;	//	𖹸	0xf0 0x96 0xb9 0xb8	MEDEFAIDRIN SMALL LETTER X
					break;
				case 0x16E59:		//	𖹙	0xf0 0x96 0xb9 0x99	MEDEFAIDRIN CAPITAL LETTER D
					*p = 0x16E79;	//	𖹹	0xf0 0x96 0xb9 0xb9	MEDEFAIDRIN SMALL LETTER D
					break;
				case 0x16E5A:		//	𖹚	0xf0 0x96 0xb9 0x9a	MEDEFAIDRIN CAPITAL LETTER OE
					*p = 0x16E7A;	//	𖹺	0xf0 0x96 0xb9 0xba	MEDEFAIDRIN SMALL LETTER OE
					break;
				case 0x16E5B:		//	𖹛	0xf0 0x96 0xb9 0x9b	MEDEFAIDRIN CAPITAL LETTER N
					*p = 0x16E7B;	//	𖹻	0xf0 0x96 0xb9 0xbb	MEDEFAIDRIN SMALL LETTER N
					break;
				case 0x16E5C:		//	𖹜	0xf0 0x96 0xb9 0x9c	MEDEFAIDRIN CAPITAL LETTER R
					*p = 0x16E7C;	//	𖹼	0xf0 0x96 0xb9 0xbc	MEDEFAIDRIN SMALL LETTER R
					break;
				case 0x16E5D:		//	𖹝	0xf0 0x96 0xb9 0x9d	MEDEFAIDRIN CAPITAL LETTER O
					*p = 0x16E7D;	//	𖹽	0xf0 0x96 0xb9 0xbd	MEDEFAIDRIN SMALL LETTER O
					break;
				case 0x16E5E:		//	𖹞	0xf0 0x96 0xb9 0x9e	MEDEFAIDRIN CAPITAL LETTER AI
					*p = 0x16E7E;	//	𖹾	0xf0 0x96 0xb9 0xbe	MEDEFAIDRIN SMALL LETTER AI
					break;
				case 0x16E5F:		//	𖹟	0xf0 0x96 0xb9 0x9f	MEDEFAIDRIN CAPITAL LETTER Y
					*p = 0x16E7F;	//	𖹿	0xf0 0x96 0xb9 0xbf	MEDEFAIDRIN SMALL LETTER Y
					break;
				case 0x1E900:		//	𞤀	0xf0 0x9e 0xa4 0x80	ADLAM CAPITAL LETTER ALIF
					*p = 0x1E922;	//	𞤢	0xf0 0x9e 0xa4 0xa2	ADLAM SMALL LETTER ALIF
					break;
				case 0x1E901:		//	𞤁	0xf0 0x9e 0xa4 0x81	ADLAM CAPITAL LETTER DAALI
					*p = 0x1E923;	//	𞤣	0xf0 0x9e 0xa4 0xa3	ADLAM SMALL LETTER DAALI
					break;
				case 0x1E902:		//	𞤂	0xf0 0x9e 0xa4 0x82	ADLAM CAPITAL LETTER LAAM
					*p = 0x1E924;	//	𞤤	0xf0 0x9e 0xa4 0xa4	ADLAM SMALL LETTER LAAM
					break;
				case 0x1E903:		//	𞤃	0xf0 0x9e 0xa4 0x83	ADLAM CAPITAL LETTER MIIM
					*p = 0x1E925;	//	𞤥	0xf0 0x9e 0xa4 0xa5	ADLAM SMALL LETTER MIIM
					break;
				case 0x1E904:		//	𞤄	0xf0 0x9e 0xa4 0x84	ADLAM CAPITAL LETTER BA
					*p = 0x1E926;	//	𞤦	0xf0 0x9e 0xa4 0xa6	ADLAM SMALL LETTER BA
					break;
				case 0x1E905:		//	𞤅	0xf0 0x9e 0xa4 0x85	ADLAM CAPITAL LETTER SINNYIIYHE
					*p = 0x1E927;	//	𞤧	0xf0 0x9e 0xa4 0xa7	ADLAM SMALL LETTER SINNYIIYHE
					break;
				case 0x1E906:		//	𞤆	0xf0 0x9e 0xa4 0x86	ADLAM CAPITAL LETTER PE
					*p = 0x1E928;	//	𞤨	0xf0 0x9e 0xa4 0xa8	ADLAM SMALL LETTER PE
					break;
				case 0x1E907:		//	𞤇	0xf0 0x9e 0xa4 0x87	ADLAM CAPITAL LETTER BHE
					*p = 0x1E929;	//	𞤩	0xf0 0x9e 0xa4 0xa9	ADLAM SMALL LETTER BHE
					break;
				case 0x1E908:		//	𞤈	0xf0 0x9e 0xa4 0x88	ADLAM CAPITAL LETTER RA
					*p = 0x1E92A;	//	𞤪	0xf0 0x9e 0xa4 0xaa	ADLAM SMALL LETTER RA
					break;
				case 0x1E909:		//	𞤉	0xf0 0x9e 0xa4 0x89	ADLAM CAPITAL LETTER E
					*p = 0x1E92B;	//	𞤫	0xf0 0x9e 0xa4 0xab	ADLAM SMALL LETTER E
					break;
				case 0x1E90A:		//	𞤊	0xf0 0x9e 0xa4 0x8a	ADLAM CAPITAL LETTER FA
					*p = 0x1E92C;	//	𞤬	0xf0 0x9e 0xa4 0xac	ADLAM SMALL LETTER FA
					break;
				case 0x1E90B:		//	𞤋	0xf0 0x9e 0xa4 0x8b	ADLAM CAPITAL LETTER I
					*p = 0x1E92D;	//	𞤭	0xf0 0x9e 0xa4 0xad	ADLAM SMALL LETTER I
					break;
				case 0x1E90C:		//	𞤌	0xf0 0x9e 0xa4 0x8c	ADLAM CAPITAL LETTER O
					*p = 0x1E92E;	//	𞤮	0xf0 0x9e 0xa4 0xae	ADLAM SMALL LETTER O
					break;
				case 0x1E90D:		//	𞤍	0xf0 0x9e 0xa4 0x8d	ADLAM CAPITAL LETTER DHA
					*p = 0x1E92F;	//	𞤯	0xf0 0x9e 0xa4 0xaf	ADLAM SMALL LETTER DHA
					break;
				case 0x1E90E:		//	𞤎	0xf0 0x9e 0xa4 0x8e	ADLAM CAPITAL LETTER YHE
					*p = 0x1E930;	//	𞤰	0xf0 0x9e 0xa4 0xb0	ADLAM SMALL LETTER YHE
					break;
				case 0x1E90F:		//	𞤏	0xf0 0x9e 0xa4 0x8f	ADLAM CAPITAL LETTER WAW
					*p = 0x1E931;	//	𞤱	0xf0 0x9e 0xa4 0xb1	ADLAM SMALL LETTER WAW
					break;
				case 0x1E910:		//	𞤐	0xf0 0x9e 0xa4 0x90	ADLAM CAPITAL LETTER NUN
					*p = 0x1E932;	//	𞤲	0xf0 0x9e 0xa4 0xb2	ADLAM SMALL LETTER NUN
					break;
				case 0x1E911:		//	𞤑	0xf0 0x9e 0xa4 0x91	ADLAM CAPITAL LETTER KAF
					*p = 0x1E933;	//	𞤳	0xf0 0x9e 0xa4 0xb3	ADLAM SMALL LETTER KAF
					break;
				case 0x1E912:		//	𞤒	0xf0 0x9e 0xa4 0x92	ADLAM CAPITAL LETTER YA
					*p = 0x1E934;	//	𞤴	0xf0 0x9e 0xa4 0xb4	ADLAM SMALL LETTER YA
					break;
				case 0x1E913:		//	𞤓	0xf0 0x9e 0xa4 0x93	ADLAM CAPITAL LETTER U
					*p = 0x1E935;	//	𞤵	0xf0 0x9e 0xa4 0xb5	ADLAM SMALL LETTER U
					break;
				case 0x1E914:		//	𞤔	0xf0 0x9e 0xa4 0x94	ADLAM CAPITAL LETTER JIIM
					*p = 0x1E936;	//	𞤶	0xf0 0x9e 0xa4 0xb6	ADLAM SMALL LETTER JIIM
					break;
				case 0x1E915:		//	𞤕	0xf0 0x9e 0xa4 0x95	ADLAM CAPITAL LETTER CHI
					*p = 0x1E937;	//	𞤷	0xf0 0x9e 0xa4 0xb7	ADLAM SMALL LETTER CHI
					break;
				case 0x1E916:		//	𞤖	0xf0 0x9e 0xa4 0x96	ADLAM CAPITAL LETTER HA
					*p = 0x1E938;	//	𞤸	0xf0 0x9e 0xa4 0xb8	ADLAM SMALL LETTER HA
					break;
				case 0x1E917:		//	𞤗	0xf0 0x9e 0xa4 0x97	ADLAM CAPITAL LETTER QAAF
					*p = 0x1E939;	//	𞤹	0xf0 0x9e 0xa4 0xb9	ADLAM SMALL LETTER QAAF
					break;
				case 0x1E918:		//	𞤘	0xf0 0x9e 0xa4 0x98	ADLAM CAPITAL LETTER GA
					*p = 0x1E93A;	//	𞤺	0xf0 0x9e 0xa4 0xba	ADLAM SMALL LETTER GA
					break;
				case 0x1E919:		//	𞤙	0xf0 0x9e 0xa4 0x99	ADLAM CAPITAL LETTER NYA
					*p = 0x1E93B;	//	𞤻	0xf0 0x9e 0xa4 0xbb	ADLAM SMALL LETTER NYA
					break;
				case 0x1E91A:		//	𞤚	0xf0 0x9e 0xa4 0x9a	ADLAM CAPITAL LETTER TU
					*p = 0x1E93C;	//	𞤼	0xf0 0x9e 0xa4 0xbc	ADLAM SMALL LETTER TU
					break;
				case 0x1E91B:		//	𞤛	0xf0 0x9e 0xa4 0x9b	ADLAM CAPITAL LETTER NHA
					*p = 0x1E93D;	//	𞤽	0xf0 0x9e 0xa4 0xbd	ADLAM SMALL LETTER NHA
					break;
				case 0x1E91C:		//	𞤜	0xf0 0x9e 0xa4 0x9c	ADLAM CAPITAL LETTER VA
					*p = 0x1E93E;	//	𞤾	0xf0 0x9e 0xa4 0xbe	ADLAM SMALL LETTER VA
					break;
				case 0x1E91D:		//	𞤝	0xf0 0x9e 0xa4 0x9d	ADLAM CAPITAL LETTER KHA
					*p = 0x1E93F;	//	𞤿	0xf0 0x9e 0xa4 0xbf	ADLAM SMALL LETTER KHA
					break;
				case 0x1E91E:		//	𞤞	0xf0 0x9e 0xa4 0x9e	ADLAM CAPITAL LETTER GBE
					*p = 0x1E940;	//	𞥀	0xf0 0x9e 0xa5 0x80	ADLAM SMALL LETTER GBE
					break;
				case 0x1E91F:		//	𞤟	0xf0 0x9e 0xa4 0x9f	ADLAM CAPITAL LETTER ZAL
					*p = 0x1E941;	//	𞥁	0xf0 0x9e 0xa5 0x81	ADLAM SMALL LETTER ZAL
					break;
				case 0x1E920:		//	𞤠	0xf0 0x9e 0xa4 0xa0	ADLAM CAPITAL LETTER KPO
					*p = 0x1E942;	//	𞥂	0xf0 0x9e 0xa5 0x82	ADLAM SMALL LETTER KPO
					break;
				case 0x1E921:		//	𞤡	0xf0 0x9e 0xa4 0xa1	ADLAM CAPITAL LETTER SHA
					*p = 0x1E943;	//	𞥃	0xf0 0x9e 0xa5 0x83	ADLAM SMALL LETTER SHA
					break;
				}
			}
		p++;
		}
	}
	return pUtf32;
}
/*******************************************************************************************

UTF32

*******************************************************************************************/
size_t StrLenUtf32(const Utf32Char* str)
{
	Utf32Char* s;
	for (s = (Utf32Char*)str; *s; ++s);
	return(s - str);
}
Utf32Char* StrCpyUtf32(Utf32Char* dest, const Utf32Char* src)
{
	return (Utf32Char *)memcpy(dest, src, (StrLenUtf32(src) + 1)*sizeof(Utf32Char));
}
Utf32Char* StrCatUtf32(Utf32Char* dest, const Utf32Char* src)
{
	return StrCpyUtf32(dest + StrLenUtf32(dest), src);
}
int StrnCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2, size_t ztCount)
{
	if (Utf32s1 && *Utf32s1 && Utf32s2 && *Utf32s2) {
		for (; ztCount--; Utf32s1++, Utf32s2++) {
			int iDiff = *Utf32s1 - *Utf32s2;
			if (iDiff != 0 || !*Utf32s1 || !*Utf32s2)
				return iDiff;
		}
		return 0;
	}
	return (-1);
}
int StrnCiCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2, size_t ztCount)
{
	Utf32Char* pStr1Low = 0;
	Utf32Char* pStr2Low = 0;
	int iResult = (-1);

	if (Utf32s1 && *Utf32s1 && Utf32s2 && *Utf32s2) {
		pStr1Low = (Utf32Char*)calloc(StrLenUtf32(Utf32s1) + 1, sizeof(Utf32Char));
		if (pStr1Low) {
			pStr2Low = (Utf32Char*)calloc(StrLenUtf32(Utf32s2) + 1, sizeof(Utf32Char));
			if (pStr2Low) {
				memcpy((Utf32Char*)pStr1Low, Utf32s1, StrLenUtf32(Utf32s1) * sizeof(Utf32Char));
				memcpy((Utf32Char*)pStr2Low, Utf32s2, StrLenUtf32(Utf32s2) * sizeof(Utf32Char));
				StrToLwrUtf32(pStr1Low);
				StrToLwrUtf32(pStr2Low);
				iResult = StrnCmpUtf32(pStr1Low, pStr2Low, ztCount);
				free(pStr1Low);
				free(pStr2Low);
			}
			else
				free(pStr1Low);
		}
	}
	return iResult;
}
int StrCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2)
{
	return StrnCmpUtf32(Utf32s1, Utf32s2, (size_t)(-1));
}
int StrCiCmpUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2)
{
	return StrnCiCmpUtf32(Utf32s1, Utf32s2, (size_t)(-1));
}
Utf32Char* StrCiStrUtf32(const Utf32Char* Utf32s1, const Utf32Char* Utf32s2)
{
	Utf32Char* p = (Utf32Char*)Utf32s1;
	size_t len = 0;

	if (Utf32s1 && *Utf32s1 && Utf32s2 && *Utf32s2) {
		len = StrLenUtf32(Utf32s2);
		while (*p) {
			if (StrnCiCmpUtf32(p, Utf32s2, len) == 0)
				return (Utf32Char*)p;
			p++;
		}
	}
	return (0);
}
/*******************************************************************************************

UTF16

*******************************************************************************************/
size_t StrLenUtf16(const Utf16Char* str)
{
	Utf16Char* s;
	for (s = (Utf16Char*)str; *s; ++s);
	return(s - str);
}
Utf16Char* StrCpyUtf16(Utf16Char* dest, const Utf16Char* src)
{
	return (Utf16Char*)memcpy(dest, src, (StrLenUtf16(src) + 1) * sizeof(Utf16Char));
}
Utf16Char* StrCatUtf16(Utf16Char* dest, const Utf16Char* src)
{
	return StrCpyUtf16(dest + StrLenUtf16(dest), src);
}
int StrnCmpUtf16(const Utf16Char* Utf16s1, const Utf16Char* Utf16s2, size_t ztCount)
{
	if (Utf16s1 && *Utf16s1 && Utf16s2 && *Utf16s2) {
		for (; ztCount--; Utf16s1++, Utf16s2++) {
			int iDiff = (int)(*Utf16s1 - *Utf16s2);
			if (iDiff != 0 || !*Utf16s1 || !*Utf16s2)
				return iDiff;
		}
		return 0;
	}
	return (-1);
}
int StrCmpUtf16(const Utf16Char* Utf16s1, const Utf16Char* Utf16s2)
{
	return StrnCmpUtf16(Utf16s1, Utf16s2, (size_t)(-1));
}
size_t CharLenUtf16(const Utf16Char* pUtf16)
{
	size_t ztUtf16 = 0;
	size_t ztNumUtf16Chars = 0;

	while (pUtf16[ztUtf16] != 0) {
		ztNumUtf16Chars++;

		if (!(((uint16_t)pUtf16[ztUtf16] - 0xd800u) < 0x800u))
			// 1 byte code point
			ztUtf16 += 1;
		else if ((((uint16_t)pUtf16[ztUtf16] & 0xfffffc00u) == 0xd800u) && ((((uint16_t)pUtf16[ztUtf16 + 1] & 0xfffffc00u) == 0xdc00u)))
			// 2 byte code point
			ztUtf16 += 2;
	}
	return ztNumUtf16Chars;
}
Utf16Char* ForwardUtf16Chars(const Utf16Char* pUtf16, size_t ztForwardUtf16Chars)
{
	size_t ztUtf16 = 0;
	size_t ztNumUtf16Chars = 0;

	while ((ztNumUtf16Chars < ztForwardUtf16Chars) && ((uint16_t)pUtf16[ztUtf16] != 0)) {
		ztNumUtf16Chars++;
		if (!(((uint16_t)pUtf16[ztUtf16] - 0xd800u) < 0x800u))
			// 1 byte code point
			ztUtf16 += 1;
		else if ((((uint16_t)pUtf16[ztUtf16] & 0xfffffc00u) == 0xd800u) && ((((uint16_t)pUtf16[ztUtf16 + 1] & 0xfffffc00u) == 0xdc00u)))
			// 2 byte code point
			ztUtf16 += 2;
	}
	return (Utf16Char*)&pUtf16[ztUtf16];
}
size_t StrLenUtf32AsUtf16(const Utf32Char* pUtf32)
{
	size_t ztUtf32 = 0;
	size_t ztNumUtf16Chars = 0;
	while (pUtf32[ztUtf32] != 0) {
		if ((pUtf32[ztUtf32] >= 0x10000u)
			&& (pUtf32[ztUtf32] <= 0x10FFFFu))
			// 2 byte code point
			ztNumUtf16Chars++;
		ztNumUtf16Chars++;
		ztUtf32++;
	}
	return ztNumUtf16Chars;
}
Utf16Char* Utf32ToUtf16(const Utf32Char* pUtf32)
{
	size_t ztUtf32 = 0;
	Utf16Char* pUtf16 = (Utf16Char*)calloc(StrLenUtf32AsUtf16(pUtf32) + 1, sizeof(Utf16Char));
	if (pUtf16) {
		// https://datatracker.ietf.org/doc/html/rfc3629#page-4
		// This function skips data when UTF bytes are out of bounds
		while (pUtf32[ztUtf32] != 0) {
			if (pUtf32[ztUtf32] < 0x10000u)
				pUtf16[StrLenUtf16((const Utf16Char*)pUtf16)] = (Utf16Char)pUtf32[ztUtf32];
			else if (pUtf32[ztUtf32] <= 0x10FFFFu) {
				pUtf16[StrLenUtf16((const Utf16Char*)pUtf16)] = (Utf16Char)(((((uint32_t)pUtf32[ztUtf32] - 0x10000u) << 12) >> 22) + 0xD800u);
				pUtf16[StrLenUtf16((const Utf16Char*)pUtf16)] = (Utf16Char)(((((uint32_t)pUtf32[ztUtf32] - 0x10000u) << 22) >> 22) + 0xDC00u);
			}
			// else out of RFC3629 Utf16 bounds doing nothing
			ztUtf32++;
		}
		return pUtf16;
	}
	return 0;
}
Utf32Char* Utf16ToUtf32(const Utf16Char* pUtf16)
{
	size_t ztUtf16 = 0;
	size_t ztUtf32 = 0;
	Utf32Char* pUtf32 = (Utf32Char*)calloc(CharLenUtf16(pUtf16) + 1, sizeof(Utf32Char));
	if (pUtf32) {
		// https://datatracker.ietf.org/doc/html/rfc3629#page-4
		// This function skips data when UTF bytes are out of bounds
		while ((uint16_t)pUtf16[ztUtf16] != 0) {
			if (!(((uint16_t)pUtf16[ztUtf16] - 0xd800u) < 0x800u)) { // The same just copy
				pUtf32[ztUtf32] = (uint16_t)pUtf16[ztUtf16];
				ztUtf16++;
			}
			else if ((((uint16_t)pUtf16[ztUtf16] & 0xfffffc00u) == 0xd800u) && ((((uint16_t)pUtf16[ztUtf16 + 1] & 0xfffffc00u) == 0xdc00u))) {
				pUtf32[ztUtf32] = ((uint16_t)pUtf16[ztUtf16] << 10) + (uint16_t)pUtf16[ztUtf16 + 1] - 0x35fdc00u;
				ztUtf16 += 2;
			}
			// else ERROR
			ztUtf32++;
		}
		return pUtf32;
	}
	return 0;
}
Utf8Char* Utf16ToUtf8(const Utf16Char* pUtf16)
{
	Utf32Char* pUtf32 = Utf16ToUtf32((const Utf16Char*)pUtf16);
	if (pUtf32) {
		Utf8Char* pUtf8 = Utf32ToUtf8((const Utf32Char*)pUtf32);
		free(pUtf32);
		if (pUtf8)
			return pUtf8;
		return 0;
	}
	return 0;
}
Utf16Char* Utf16StrMakeUprUtf16Str(const Utf16Char* pUtf16)
{
	Utf32Char* pUtf32 = Utf16ToUtf32(pUtf16);
	if (pUtf32 && *pUtf32) {
		StrToUprUtf32(pUtf32);
		{
			Utf16Char* pUtf16Upr = Utf32ToUtf16(pUtf32);
			free(pUtf32);
			return pUtf16Upr;
		}
	}
	return 0; // calloc() failure
}
Utf16Char* Utf16StrMakeLwrUtf16Str(const Utf16Char* pUtf16)
{
	Utf32Char* pUtf32 = Utf16ToUtf32(pUtf16);
	if (pUtf32 && *pUtf32) {
		StrToLwrUtf32(pUtf32);
		{
			Utf16Char* pUtf16Lwr = Utf32ToUtf16(pUtf32);
			free(pUtf32);
			return pUtf16Lwr;
		}
	}
	return 0; // calloc() failure
}
int StrnCiCmpUtf16(const Utf16Char* pUtf16s1, const Utf16Char* pUtf16s2, size_t ztCount)
{
	Utf32Char* pUtf32s1 = Utf16ToUtf32(pUtf16s1);
	if (pUtf32s1) {
		Utf32Char* pUtf32s2 = Utf16ToUtf32(pUtf16s2);
		if (pUtf32s2) {
			int iDiff = StrnCiCmpUtf32(pUtf32s1, pUtf32s2, ztCount);
			free(pUtf32s1);
			free(pUtf32s2);
			return iDiff;
		}
		free(pUtf32s1);
		return (-1); // calloc() failure
	}
	return (-1); // calloc() failure
}
int StrCiCmpUtf16(const Utf16Char* pUtf16s1, const Utf16Char* pUtf16s2)
{
	return StrnCiCmpUtf16(pUtf16s1, pUtf16s2, (size_t)(-1));
}
Utf16Char* StrCiStrUtf16(const Utf16Char* pUtf16s1, const Utf16Char* pUtf16s2)
{
	Utf32Char* pUtf32s1 = Utf16ToUtf32(pUtf16s1);
	if (pUtf32s1) {
		Utf32Char* pUtf32s2 = Utf16ToUtf32(pUtf16s2);
		Utf32Char* pUtf32Found = StrCiStrUtf32(pUtf32s1, pUtf32s2);
		if (pUtf32s2) {
			Utf16Char* pUtf16Found = 0;
			size_t ztSteps = pUtf32Found - pUtf32s1;
			if (pUtf32Found)
				pUtf16Found = ForwardUtf16Chars(pUtf16s1, ztSteps);
			free(pUtf32s1);
			free(pUtf32s2);
			return pUtf16Found;
		}
		free(pUtf32s1);
		return 0; // calloc() failure
	}
	return 0; // calloc() failure
}
/*******************************************************************************************

UTF8

*******************************************************************************************/
size_t StrLenUtf8(const Utf8Char* str)
{
	Utf8Char* s;
	for (s = (Utf8Char*)str; *s; ++s);
	return(s - str);
}
int StrnCmpUtf8(const Utf8Char* Utf8s1, const Utf8Char* Utf8s2, size_t ztCount)
{
	if (Utf8s1 && *Utf8s1 && Utf8s2 && *Utf8s2) {
		for (; ztCount--; Utf8s1++, Utf8s2++) {
			int iDiff = *Utf8s1 - *Utf8s2;
			if (iDiff != 0 || !*Utf8s1 || !*Utf8s2)
				return iDiff;
		}
		return 0;
	}
	return (-1);
}
int StrCmpUtf8(const Utf8Char* Utf8s1, const Utf8Char* Utf8s2)
{
	return StrnCmpUtf8(Utf8s1, Utf8s2, (size_t)(-1));
}
size_t CharLenUtf8(const Utf8Char* pUtf8)
{
	size_t ztUtf8 = 0;
	size_t ztNumUtf8Chars = 0;

	while (pUtf8[ztUtf8] != 0) {
		ztNumUtf8Chars++;

		if ((pUtf8[ztUtf8] & 0b10000000) == 0)
			// 1 byte code point
			ztUtf8 += 1;
		else if ((pUtf8[ztUtf8] & 0b11100000) == 0b11000000)
			// 2 byte code point
			ztUtf8 += 2;
		else if ((pUtf8[ztUtf8] & 0b11110000) == 0b11100000)
			// 3 byte code point
			ztUtf8 += 3;
		else
			// 4 byte code point
			ztUtf8 += 4;
	}
	return ztNumUtf8Chars;
}
Utf8Char* ForwardUtf8Chars(const Utf8Char* pUtf8, size_t ztForwardUtf8Chars)
{
	size_t ztUtf8 = 0;
	size_t ztNumUtf8Chars = 0;

	while ((ztNumUtf8Chars < ztForwardUtf8Chars) && (pUtf8[ztUtf8] != 0)) {
		ztNumUtf8Chars++;

		if ((pUtf8[ztUtf8] & 0b10000000) == 0)
			// 1 byte code point
			ztUtf8 += 1;
		else if ((pUtf8[ztUtf8] & 0b11100000) == 0b11000000)
			// 2 byte code point
			ztUtf8 += 2;
		else if ((pUtf8[ztUtf8] & 0b11110000) == 0b11100000)
			// 3 byte code point
			ztUtf8 += 3;
		else
			// 4 byte code point
			ztUtf8 += 4;
	}
	return (Utf8Char*)&pUtf8[ztUtf8];
}
size_t StrLenUtf32AsUtf8(const Utf32Char* pUtf32)
{
	size_t ztUtf32 = 0;
	size_t ztNumUtf8Chars = 0;
	while (pUtf32[ztUtf32] != 0) {
		if (pUtf32[ztUtf32] >= 0x10000u)
			ztNumUtf8Chars += 3;
		else if (pUtf32[ztUtf32] >= 0x800u)
			ztNumUtf8Chars += 2;
		else if (pUtf32[ztUtf32] >= 0x80u)
			ztNumUtf8Chars += 1;
		ztNumUtf8Chars++;
		ztUtf32++;
	}
	return ztNumUtf8Chars;
}
Utf8Char* Utf32ToUtf8(const Utf32Char* pUtf32)
{
	size_t ztUtf32 = 0;
	Utf8Char* pUtf8 = (Utf8Char*)calloc(StrLenUtf32AsUtf8(pUtf32) + 1, sizeof(Utf8Char));
	if (pUtf8) {
		// https://datatracker.ietf.org/doc/html/rfc3629#page-4
		// This function skips data when UTF bytes are out of bounds
		while (pUtf32[ztUtf32] != 0) {
			if (pUtf32[ztUtf32] <= 0x7Fu) {
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)pUtf32[ztUtf32];
			}
			else if (pUtf32[ztUtf32] <= 0x7FFu) {
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0xC0u | (pUtf32[ztUtf32] >> 6));				/* 110xxxxx */
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0x80u | (pUtf32[ztUtf32] & 0x3Fu));			/* 10xxxxxx */
			}
			else if (pUtf32[ztUtf32] <= 0xFFFFu) {
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0xE0u | (pUtf32[ztUtf32] >> 12));			/* 1110xxxx */
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0x80u | ((pUtf32[ztUtf32] >> 6) & 0x3Fu));	/* 10xxxxxx */
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0x80u | (pUtf32[ztUtf32] & 0x3Fu));			/* 10xxxxxx */
			}
			else if (pUtf32[ztUtf32] <= 0x10FFFFu) {
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0xF0u | (pUtf32[ztUtf32] >> 18));			/* 11110xxx */
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0x80u | ((pUtf32[ztUtf32] >> 12) & 0x3Fu));	/* 10xxxxxx */
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0x80u | ((pUtf32[ztUtf32] >> 6) & 0x3Fu));	/* 10xxxxxx */
				pUtf8[StrLenUtf8((const Utf8Char*)pUtf8)] = (Utf8Char)(0x80u | (pUtf32[ztUtf32] & 0x3Fu));			/* 10xxxxxx */
			}
			// else out of RFC3629 UTF8 bounds doing nothing
			ztUtf32++;
		}
		return pUtf8;
	}
	return 0;
}
Utf32Char* Utf8ToUtf32(const Utf8Char* pUtf8)
{
	size_t ztUtf8 = 0;
	size_t ztUtf32 = 0;
	Utf32Char* pUtf32 = (Utf32Char*)calloc(CharLenUtf8(pUtf8) + 1, sizeof(Utf32Char));
	if (pUtf32) {
		// https://datatracker.ietf.org/doc/html/rfc3629#page-4
		// This function skips data when UTF bytes are out of bounds
		while (pUtf8[ztUtf8] != 0) {
			if ((pUtf8[ztUtf8] & 0b10000000) == 0) {
				// 1 byte code point, ASCII
				pUtf32[ztUtf32] = (pUtf8[ztUtf8] & 0b01111111);
				ztUtf8 += 1;
			}
			else if ((pUtf8[ztUtf8] & 0b11100000) == 0b11000000) {
				// 2 byte code point
				pUtf32[ztUtf32] = (pUtf8[ztUtf8] & 0b00011111) << 6 | (pUtf8[ztUtf8 + 1] & 0b00111111);
				if (pUtf32[ztUtf32] < 0x80u)				// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				else if (pUtf32[ztUtf32] > 0x7ffu)		// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				ztUtf8 += 2;
			}
			else if ((pUtf8[ztUtf8] & 0b11110000) == 0b11100000) {
				// 3 byte code point
				pUtf32[ztUtf32] = (pUtf8[ztUtf8] & 0b00001111) << 12 | (pUtf8[ztUtf8 + 1] & 0b00111111) << 6 | (pUtf8[ztUtf8 + 2] & 0b00111111);
				if (pUtf32[ztUtf32] < 0x800u)			// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				else if (pUtf32[ztUtf32] > 0xffffu)		// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				ztUtf8 += 3;
			}
			else {
				// 4 byte code point
				if (pUtf8[ztUtf8] <= 0xf4u)				// RFC3629 UTF8 do not allow larger values
					pUtf32[ztUtf32] = (pUtf8[ztUtf8] & 0b00000111) << 18 | (pUtf8[ztUtf8 + 1] & 0b00111111) << 12 | (pUtf8[ztUtf8 + 2] & 0b00111111) << 6 | (pUtf8[ztUtf8 + 3] & 0b00111111);
				else									// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				if (pUtf32[ztUtf32] < 0x10000u)			// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				else if (pUtf32[ztUtf32] > 0x10FFFFu)	// Not a valid result, Wrong encoding
					pUtf32[ztUtf32] = 0;				// Out of UTF8 bound, skip data
				ztUtf8 += 4;
			}
			if (pUtf32[ztUtf32]) // Only step when valid data
				ztUtf32++;
		}
		return pUtf32;
	}
	return 0;
}
Utf16Char* Utf8ToUtf16(const Utf8Char* pUtf8)
{
	Utf32Char* pUtf32 = Utf8ToUtf32((const Utf8Char*)pUtf8);
	if (pUtf32) {
		Utf16Char* pUtf16 = Utf32ToUtf16((const Utf32Char*)pUtf32);
		free(pUtf32);
		if (pUtf16)
			return pUtf16;
		return 0;
	}
	return 0;
}
Utf8Char* Utf8StrMakeUprUtf8Str(const Utf8Char* pUtf8)
{
	Utf32Char* pUtf32 = Utf8ToUtf32(pUtf8);
	if (pUtf32 && *pUtf32) {
		StrToUprUtf32(pUtf32);
		{
			Utf8Char* pUtf8Upr = Utf32ToUtf8(pUtf32);
			free(pUtf32);
			return pUtf8Upr;
		}
	}
	return 0; // calloc() failure
}
Utf8Char* Utf8StrMakeLwrUtf8Str(const Utf8Char* pUtf8)
{
	Utf32Char* pUtf32 = Utf8ToUtf32(pUtf8);
	if (pUtf32 && *pUtf32) {
		StrToLwrUtf32(pUtf32);
		{
			Utf8Char* pUtf8Lwr = Utf32ToUtf8(pUtf32);
			free(pUtf32);
			return pUtf8Lwr;
		}
	}
	return 0; // calloc() failure
}
int StrnCiCmpUtf8(const Utf8Char* pUtf8s1, const Utf8Char* pUtf8s2, size_t ztCount)
{
	Utf32Char* pUtf32s1 = Utf8ToUtf32(pUtf8s1);
	if (pUtf32s1) {
		Utf32Char* pUtf32s2 = Utf8ToUtf32(pUtf8s2);
		if (pUtf32s2) {
			int iDiff = StrnCiCmpUtf32(pUtf32s1, pUtf32s2, ztCount);
			free(pUtf32s1);
			free(pUtf32s2);
			return iDiff;
		}
		free(pUtf32s1);
		return (-1); // calloc() failure
	}
	return (-1); // calloc() failure
}
int StrCiCmpUtf8(const Utf8Char* pUtf8s1, const Utf8Char* pUtf8s2)
{
	return StrnCiCmpUtf8(pUtf8s1, pUtf8s2, (size_t)(-1));
}
Utf8Char* StrCiStrUtf8(const Utf8Char* pUtf8s1, const Utf8Char* pUtf8s2)
{
	Utf32Char* pUtf32s1 = Utf8ToUtf32(pUtf8s1);
	if (pUtf32s1) {
		Utf32Char* pUtf32s2 = Utf8ToUtf32(pUtf8s2);
		Utf32Char* pUtf32Found = StrCiStrUtf32(pUtf32s1, pUtf32s2);
		if (pUtf32s2) {
			Utf8Char* pUtf8Found = 0;
			size_t ztSteps = pUtf32Found - pUtf32s1;
			if (pUtf32Found)
				pUtf8Found = ForwardUtf8Chars(pUtf8s1, ztSteps);
			free(pUtf32s1);
			free(pUtf32s2);
			return pUtf8Found;
		}
		free(pUtf32s1);
		return 0; // calloc() failure
	}
	return 0; // calloc() failure
}
#ifdef UTF32_TEST
/******************/
/* Test functions */
/******************/
#include <stdio.h>
size_t FileWrite(char* pFileName, char* pBuffer)
{
	/* Open the file on disk */
	FILE* pFile = fopen((const char*)pFileName, (const char*)"wb");
	if (!pFile)
		return 0; /* File not found or opened */
	{
		size_t ztWritten = fwrite(pBuffer, sizeof(char), StrLenUtf8(pBuffer), pFile);
		fclose(pFile);
		return ztWritten;
	}
}

int main(void) {

	Utf8Char Base8[] = { "Text ÆÇÈ𐒷𐒶𐐝ＴꞫ123ÉêßꞵëìíîïðÑÒÓÔÕÖØÙÚÛÜÝÞĀĂĄ" };
	Utf8Char Target8[] = { "𐓞𐑅ＴꞫ123éêßꞵë" };
	Utf8Char Copy8[] = { "Hello 𐓞𐑅ＴꞫ123éêßꞵë123" };
	Utf8Char* pLwr8 = Utf8StrMakeLwrUtf8Str(Base8);
	Utf8Char* pUpr8 = Utf8StrMakeUprUtf8Str(pLwr8);
	Utf8Char* pLwr8B = Utf8StrMakeLwrUtf8Str(pUpr8);
	int iDiff8 = StrnCiCmpUtf8(pLwr8, pUpr8, 2);
	int iDiff8B = StrCiCmpUtf8(pLwr8, pUpr8);
	Utf8Char* pSearch8 = StrCiStrUtf8(Base8, Target8);

	Utf16Char* pBase16 = Utf8ToUtf16(Base8);
	Utf16Char* pTarget16 = Utf8ToUtf16(Target8);
	Utf16Char* pLwr16 = Utf16StrMakeLwrUtf16Str(pBase16);
	Utf16Char* pUpr16 = Utf16StrMakeUprUtf16Str(pLwr16);
	Utf16Char* pLwr16B = Utf16StrMakeLwrUtf16Str(pUpr16);
	int iDiff16 = StrnCiCmpUtf16(pLwr16, pUpr16, 2);
	int iDiff16B = StrCiCmpUtf16(pLwr16, pUpr16);
	Utf16Char* pSearch16 = StrCiStrUtf16(pBase16, pTarget16);

	Utf16Char* pCopy16 = Utf8ToUtf16(Copy8);
	Utf16Char* pCopyTest16 = calloc(200, sizeof(Utf16Char));
	StrCpyUtf16(pCopyTest16, pTarget16);
	StrCpyUtf16(pCopyTest16, pCopy16);
	StrCatUtf16(pCopyTest16, pTarget16);

	Utf32Char* pTarget32 = Utf8ToUtf32(Target8);
	Utf32Char* pCopy32 = Utf8ToUtf32(Copy8);
	Utf32Char* pCopyTest32 = calloc(200, sizeof(Utf32Char));
	StrCpyUtf32(pCopyTest32, pTarget32);
	StrCpyUtf32(pCopyTest32, pCopy32);
	StrCatUtf32(pCopyTest32, pTarget32);

	Utf8Char* pBase16_8 = Utf16ToUtf8(pBase16);
	Utf8Char* pLwr16_8 = Utf16ToUtf8(pLwr16);
	Utf8Char* pUpr16_8 = Utf16ToUtf8(pUpr16);
	Utf8Char* pLwr16B_8 = Utf16ToUtf8(pLwr16B);
	Utf8Char* pTarget16_8 = Utf16ToUtf8(pTarget16);
	Utf8Char* pSearch16_8 = Utf16ToUtf8(pSearch16);

	Utf8Char* pCopy16_8 = Utf16ToUtf8(pCopy16);
	Utf8Char* pCopyTest16_8 = Utf16ToUtf8(pCopyTest16);
	Utf8Char* pCopy32_8 = Utf32ToUtf8(pCopy32);
	Utf8Char* pCopyTest32_8 = Utf32ToUtf8(pCopyTest32);

	char Buffer[10000];
	sprintf(Buffer,"\xEF\xBB\xBF\nBase8  = %s\nBase16 = %s\npUpr8  = %s\npUpr16 = %s\npLwr8  = %s\npLwr8B = %s\npLwr16 = %s\npLwr16B= %s\npTarget8  = %s\npTarget16 = %s\npSearch8  = %s\npSearch16 = %s\nDiff8 = %i\nDiff8b = %i\nDiff16 = %i\nDiff16b = %i\npCopy16_8  = %s\npCopyTest16_8  = %s\npCopy32_8  = %s\npCopyTest32_8  = %s\n",
		Base8, pBase16_8, pUpr8, pUpr16_8, pLwr8, pLwr8B, pLwr16_8, pLwr16B_8, Target8, pTarget16_8, pSearch8, pSearch16_8, iDiff8, iDiff8B, iDiff16, iDiff16B, pCopy16_8, pCopyTest16_8, pCopy32_8, pCopyTest32_8);
	printf("% s", Buffer);
	FileWrite("UTF8_checked.txt", Buffer);
	free(pLwr8);
	free(pUpr8);
	free(pLwr8B);

	free(pBase16);
	free(pTarget16);
	free(pLwr16);
	free(pUpr16);
	free(pLwr16B);

	free(pBase16_8);
	free(pLwr16_8);
	free(pUpr16_8);
	free(pLwr16B_8);
	free(pTarget16_8);
	free(pSearch16_8);

	free(pCopy16);
	free(pCopyTest16);
	free(pCopy32);
	free(pCopyTest32);
	free(pCopy16_8);
	free(pCopyTest16_8);
	free(pCopy32_8);
	free(pCopyTest32_8);

	return 0;
}
#endif
