#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A script to prepare meta data json for Class 12 Books
"""

import json

info = """
1	हिन्दी	Vyavasai Adhyan-I   http://bstbpc.gov.in/Bookpdf/class12/classXllVyavasaiAdhyanI/classXllVyavasaiAdhyanI.zip
3	हिन्दी	Vyavasai Adhyan II  http://bstbpc.gov.in/Bookpdf/class12/classXll-VyavasaiAdhyanII/classXll-VyavasaiAdhyanII.zip
2	हिन्दी	Bhogol main peryojnatmak    http://bstbpc.gov.in/Bookpdf/class12/classXll_Bhogolmainperyojnatmak/classXll_Bhogolmainperyojnatmak.zip
4	हिन्दी	Bharatiya Itihas 1  http://bstbpc.gov.in/Bookpdf/class12/classXllBharatiyaItihas1/classXllBharatiyaItihas1.zip
5	हिन्दी	Bharatiya Itihas 2  http://bstbpc.gov.in/Bookpdf/class12/classXllBharatiyaItihas2/classXllBharatiyaItihas2.zip
6	हिन्दी	Bharatiya Itihas 3  http://bstbpc.gov.in/Bookpdf/class12/classXllBharatiyaItihas3/classXllBharatiyaItihas3.zip
7	हिन्दी	Bharatlog   http://bstbpc.gov.in/Bookpdf/class12/classXllBharatlog/classXllBharatlog.zip
8	हिन्दी	Bharat main Samajik http://bstbpc.gov.in/Bookpdf/class12/classXllBharatmainSamajik/classXllBharatmainSamajik.zip
9	हिन्दी	Biology http://bstbpc.gov.in/Bookpdf/class12/classXllJeevVigyan/classXllJeevVigyan.zip
10	हिन्दी	Chemistry Hndi part1    http://bstbpc.gov.in/Bookpdf/class12/ChemistryPart1/lhch1dd.zip
11	हिन्दी	Chemistry Hndi part2    http://bstbpc.gov.in/Bookpdf/class12/ChemistryPart2/lhch2dd.zip
12	हिन्दी	Lekhashastra1   http://bstbpc.gov.in/Bookpdf/class11/classxl_Lekhashastra-I/classxl_Lekhashastra-I.zip
13	हिन्दी	LekhashastraII  http://bstbpc.gov.in/Bookpdf/class11/classXlLekhashastra-II/classXlLekhashastra-II.zip
14	हिन्दी	LekhaAlavkari-1 http://bstbpc.gov.in/Bookpdf/class12/classXllLekhashastraPart-l/classXllLekhashastraPart-l.zip
15	हिन्दी	Lekhasastra Company-2   http://bstbpc.gov.in/Bookpdf/class12/classXll-VyavasaiAdhyanII/classXll-VyavasaiAdhyanII.zip
16	हिन्दी	ManavBhugol http://bstbpc.gov.in/Bookpdf/class12/classXllManavBhugol/classXllManavBhugol.zip
17	हिन्दी	Manovigyan  http://bstbpc.gov.in/Bookpdf/class12/classXllManovigyan/classXllManovigyan.zip
18	हिन्दी	Physics 1   http://bstbpc.gov.in/Bookpdf/class12/classXllBhautiki-I/classXllBhautiki-I.zip
19	हिन्दी	Physics II  http://bstbpc.gov.in/Bookpdf/class12/classXllBhautiki-ll/classXllBhautiki-ll.zip
20	हिन्दी	Physics Hindi I http://bstbpc.gov.in/Bookpdf/class11/class11Phypart1/Bhautiki-I/khph1dd.zip
21	हिन्दी	Physics Hindi II    http://bstbpc.gov.in/Bookpdf/class11/class11Phypart2/Bhautiki-II/khph2dd.zip
22	हिन्दी	Samashty Arthshastra    http://bstbpc.gov.in/Bookpdf/class12/classXllSamashtyArthshastra/classXllSamashtyArthshastra.zip
23	हिन्दी	Samkalin Vishwa http://bstbpc.gov.in/Bookpdf/class12/classXllSamkalinVishwa/classXllSamkalinVishwa.zip
24	हिन्दी	Swatantra Bharat    http://bstbpc.gov.in/Bookpdf/class12/classXllSamkalinVishwa/classXllSamkalinVishwa.zip
25	हिन्दी	Biology Hindi   http://bstbpc.gov.in/Bookpdf/class11/class11Bio/bio/khbo1dd.zip
26	हिन्दी	Math Hindi  http://bstbpc.gov.in/Bookpdf/class11/class11Math/khmh1dd.zip
27	हिन्दी	Math Hindi Part2    http://bstbpc.gov.in/Bookpdf/class11/class11Math/khmh1dd.zip
28	हिन्दी	Vyashthi Arthshasrta    http://bstbpc.gov.in/Bookpdf/class12/classXll_VyashthiArthshasrta/classXll_VyashthiArthshasrta.zip
29	हिन्दी	Vyashthi Arthshasrta    http://bstbpc.gov.in/Bookpdf/class12/classXll_VyashthiArthshasrta/classXll_VyashthiArthshasrta.zip
"""

info = info.split("\n")
info = [i.rsplit(maxsplit=1) for i in info if len(i.strip()) > 0]
info = [[i[0].split("\t"), i[1]] for i in info]

dd = dict()
dd["class12"] = list()
for i in info:
    temp = dict()
    temp["language"] = i[0][1]
    temp["bookname"] = i[0][2]
    temp["complete_book"] = i[1]
    temp["chapters"] = None

    dd["class12"].append(temp)


with open("data/fetcher_meta_data/class12books.json", "w", encoding="utf-8") as fp:
    json.dump(dd, fp, ensure_ascii=False, indent=2)

dd["class12"] = dd["class12"][:4]

with open(
    "data/fetcher_meta_data/class12books_sample_test.json", "w", encoding="utf-8"
) as fp:
    json.dump(dd, fp, ensure_ascii=False, indent=2)

print("done")
