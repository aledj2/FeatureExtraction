import rpy2.robjects as robjects

import time

i=1
r=robjects.r
r.X11()

r('x <- rnorm(1000)') # generate x in r
while i==1:
    r('plot(x)')


#time.sleep(10)