#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class Indexer_Exception(Exception):
    pass


class PDF_Not_Found_Exception(Indexer_Exception):
    pass


class Batch_Length_Mismatch_Exception(Indexer_Exception):
    pass


class Image_Not_Found_Exception(Indexer_Exception):
    pass


class OCR_Result_Directory_Absent_Error(Indexer_Exception):
    pass


class Books_Directory_Absent(Indexer_Exception):
    pass


class Exclusion_List_File_Absent(Indexer_Exception):
    pass


class ES_Connection_Failure(Indexer_Exception):
    pass
