


def binsearch(x,y,n):
    mid = int((x+y)/2)
    if n == mid:
        print('the value is:', n)

    else:
        if n > mid:
            binsearch(mid,y,n)
        else:
            binsearch(x,mid,n)



binsearch(1,100,4)

