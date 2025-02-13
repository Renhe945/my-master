


ask_prev = []
bid_prev = []

def func(time, bid, ask):
    print(time)
    print(eval(bid))
    bid_now = eval(bid)
    # print(type(bid_prev))
    # print(type(bid_now))
    global  bid_prev
    if (len(bid_now) < len(bid_prev)):
        print(bid_prev)
        print(bid_now)
    bid_prev = bid_now
    print(bid_prev)
    return float(time) * 10

