# -*- coding: utf-8 -*-
"""
Balanced Error Rate error functions
"""
__author__ = """Giovanni Colavizza"""

import numpy as np

def BER(yn, ynhat):
    """
    Implementation of Balanced Error Rate

    :param yn: ground truth
    :param ynhat: predicted values
    :return: error score

    """
    y = list()
    for z in yn:
        y.extend(z)
    yhat = list()
    for z in ynhat:
        yhat.extend(z)
    yn = np.array(y)
    ynhat = np.array(yhat)
    c = set(list(yn) + list(ynhat)) # set of unique classes
    error = 0.0
    numClasses = 0
    for C in c:
        if(len(np.array(yn == C)) != 0):
            error += np.sum(np.array(yn == C) * np.array(yn != ynhat))/float(np.sum(np.array(yn == C)))
            numClasses += 1
    if numClasses == 0: return 1.0
    error = error/numClasses
    return error

def BER_vector(yn, ynhat):
    """
    Implementation of Balanced Error Rate, returns a vector with errors for each class

    :param yn: ground truth
    :param ynhat: predicted values
    :return: error score vector, scores for each class

    """
    y = list()
    for z in yn:
        y.extend(z)
    yhat = list()
    for z in ynhat:
        yhat.extend(z)
    yn = np.array(y)
    ynhat = np.array(yhat)
    c = set(list(yn) + list(ynhat)) # set of unique classes
    error = list()
    classes = list()
    for C in c:
        if(np.sum(np.array(yn == C)) != 0):
            error.append(np.sum(np.array(yn == C) * np.array(yn != ynhat))/float(np.sum(np.array(yn == C))))
            classes.append(C)
    return error, classes