import numpy as np
from sklearn.datasets import make_blobs as blobs
from sklearn.datasets import make_gaussian_quantiles as gq

def Uniform(numOfPoints):
  x1 = np.random.uniform(-50., 50., size=(numOfPoints, 2))
  x2 = np.random.uniform(-50., 50., size=(numOfPoints, 2))
  np.savetxt('uniform1.csv', x1, delimiter=',')
  np.savetxt('uniform2.csv', x2, delimiter=',')
  return 'Done'

def Blobs(numOfPoints):
  X1, y1 = blobs(n_samples=numOfPoints, n_features=2, centers=40, center_box= (-50., 50.))
  X2, y2 = blobs(n_samples=numOfPoints, n_features=2, centers=40, center_box= (-50., 50.))
  np.savetxt('blobs1.csv', X1, delimiter=',')
  np.savetxt('blobs2.csv', X2, delimiter=',')
  return 'Done'
 
def GaussianQuantiles(numOfPoints):
  x1 , y= gq(cov = 60, n_samples=numOfPoints)
  x1 += 3
  x2 , y= gq(cov=95, n_samples=numOfPoints)
  x2 -=10
  np.savetxt('gQ1.csv', x1, delimiter=',')
  np.savetxt('gQ2.csv', x2, delimiter=',')
  return 'Done'
