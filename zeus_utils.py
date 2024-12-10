


def add_ord_loc(orders_sum,customers):
    ord = orders_sum
    cust = customers
    ord = ord.join(cust, on=["Ship to Party"], how="left")
    return ord

def fill_trucks_customer(orders, trucks, wh_cap=25):
    # These are all orders for one customer and one date. Truck should be filled one by one until all are packed or until there are no more trucks or until warehouse capacity is reached. Return orders that are not packed, trucks that are not filled and booked orders.
    # Sort orders by volume
    orders = orders.sort_values('Total Volume',ascending=False)
    trucks = trucks.sort_values('price')
    # How to sort trucks?
    cur_o = 0
    cur_t = 0
    # Da li ici po kamionu ili po narudzbini?
    # Verovatno treba imati i podatak o narudzbinama u kamionu, isto tako i u tome kojim sve kamionima ide neka veca narudzbina, i da li je cela poslata itd.
    booked_t = []#{'truck':h_id, 'status':'part-full','orders':[{o_id:o_vol}]}]
    booked_o = []#{'order':o_id, 'truck':h_id, 'volume':o_vol}]
    split_o = None #[{'order':o_id, 'remaining_v':o_vol}]
    for i,r in trucks.iterrows():
        if len(booked_t)>=wh_cap:
            break
        t_vol = r['Effective Volume'] # capacity
        h_id = r['contractNumber'] # haulier
        t = {'truck':h_id, 'status':'empty','orders':[]}
        vol = 0
        if split_o:
            vol=split_o['remaining_v']
            if vol==t_vol:
                # the remaining volume is exactly the same as the truck volume
                t['status']='full'
                booked_t.append(t)
                wh_cap-=1
                booked_o.append({'order':split_o['order'], 'truck':h_id,
                                 'volume':vol})
                cur_o+=1
                split_o = None
                continue
            elif vol>t_vol:
                # the remaining volume is larger than the truck volume
                t['orders'].append({split_o['order']:t_vol})
                t['status']='full'
                booked_t.append(t)
                wh_cap-=1
                booked_o.append({'order':split_o['order'], 'truck':h_id,
                                 'volume':t_vol})
                split_o['remaining_v'] = vol-t_vol
                continue
            else:
                # the remaining volume is smaller than the truck volume
                t['orders'].append({split_o['order']:vol})
                t['status']='part-full'
                cur_o+=1                
        while vol<t_vol and cur_o<len(orders):
            # take order
            o_id = orders.iloc[cur_o].name #['Order Number']
            o_vol = orders.iloc[cur_o]['Total Volume']
            if vol+o_vol<=t_vol:
                # add order to truck
                t['orders'].append({o_id:o_vol})
                t['status']='part-full'
                vol+=o_vol
                cur_o+=1
            else:
                t_rem_vol = t_vol-vol
                if t_rem_vol==0:
                    # the last order was exactly the same as the remaining volume
                    t['status']='full'
                    split_o = None
                    break
                # this order should be split
                # Or first try to find a smaller order that can fit?
                # Maybe this is better, analyse left volume first, and if there are no orders that can fit, split the order
                # BuT this is all for one customer,so maybe it does not matter
                o_rem_vol = o_vol-t_rem_vol
                t['orders'].append({o_id:t_rem_vol})
                t['status']='full'
                split_o = {'order':o_id, 'remaining_v':o_rem_vol}
                break
        if t['status']=='full':
            booked_t.append(t)
            wh_cap-=1
            for t_o in t['orders']:
                    booked_o.append({'order':t_o.keys()[0], 'truck':h_id,
                                     'volume':t_o.values()[0]})
        else:
            # this truck is not full, which means that there are no more orders
            break
    # Return everything booked, everything left, and split order if existing, ad wh capacity left
    bk_o_ids = [bo['order'] for bo in booked_o]
    rem_o = orders[~orders.index.isin(bk_o_ids)] # only full ord
    if split_o:
        add_o = orders.loc[split_o['order']]
        add_o['Total Volume'] = split_o['remaining_v']
        add_o['Balance Order Quantity'] = split_o['remaining_q']
        rem_o = rem_o.append(add_o)
    #rem_t = trucks[~trucks['contractNumber'].isin([bt['truck'] for bt in booked_t])]
    return booked_t, booked_o, rem_o, wh_cap