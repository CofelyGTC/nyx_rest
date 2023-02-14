#This file is part of barcodenumber. The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
'''
Check the barcodes
'''
import math

__version__ = '0.2.1'

def barcodes():
    '''
    Return the list of barcodes that have check function
    '''
    res = [x.replace('check_code_', '').upper() for x in globals()
            if x.startswith('check_code_')]
    res.sort()
    return res

def is_pair(x):
    return not x%2

def check_code_code39(number):
    '''
    Check code39 code.
    '''
    return True

def check_code_ean(number):
    '''
    Check ean code.
    '''
    return True

def check_code_ean13(number):
    '''
    Check ean13 code.
    '''
    if not number:
        return True
    if len(number) != 13:
        return False
    try:
        int(number)
    except:
        return False
    oddsum = 0
    evensum = 0
    total = 0
    eanvalue = number
    reversevalue = eanvalue[::-1]
    finalean = reversevalue[1:]

    for i in range(len(finalean)):
        if is_pair(i):
            oddsum += int(finalean[i])
        else:
            evensum += int(finalean[i])
    total=(oddsum * 3) + evensum

    check = int(10 - math.ceil(total % 10.0)) %10

    if check != int(number[-1]):
        return False
    return True


def check_code_ean8(number):
    '''
    Check ean8 code.
    '''
    return True

def check_code_gs1(number):
    '''
    Check gs1 code.
    '''
    return True

def check_code_gtin(number):
    '''
    Check gtin code.
    '''
    return True

def check_code_isbn(number):
    '''
    Check isbn code.
    '''
    return True

def check_code_isbn10(number):
    '''
    Check isbn10 code.
    '''
    if len(number) != 10:
        return False
    return True

def check_code_isbn13(number):
    '''
    Check isbn13 code.
    '''
    if len(number) != 13:
        return False
    return True

def check_code_issn(number):
    '''
    Check issn code.
    '''
    return True

def check_code_jan(number):
    '''
    Check jan code.
    '''
    return True

def check_code_pzn(number):
    '''
    Check pzn code.
    '''
    return True

def check_code_upc(number):
    '''
    Check upc code.
    '''
    return True

def check_code_upca(number):
    '''
    Check upca code.
    '''
    return True

def check_code(code, number):
    '''
    Check barcode
    '''
    try:
        checker = globals()['check_code_%s' % code.lower()]
    except KeyError:
        return False
    return checker(number)
