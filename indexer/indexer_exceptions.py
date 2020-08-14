#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class Indexer_Exception(Exception):
    pass


class PDF_Not_Found_Exception(Indexer_Exception):
    pass


class Batch_Length_Mismatch_Exception(Indexer_Exception):
    pass
