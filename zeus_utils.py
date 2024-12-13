import pandas as pd



volume_scale = 0.8

def add_ord_loc(orders_sum,customers):
    ord = orders_sum
    cust = customers
    ord = ord.join(cust, on=["Ship to Party"], how="left")
    return ord

# TODO: This is temporary, in the future a better function for truck filling is needed, which calculates more precicely volume decrease when using tire lacing (cris-cross packing), and which takes into account that bigger tires can't be packed like the smaller ones (no volume decrease). In short - a special tire packing algorithm is needed (container filling problem variant)

def fill_trucks_customer(orders, trucks, wh_cap=25, date=None):
    # These are all orders for one customer or location. Truck should be filled one by one until all are packed or until there are no more trucks or until warehouse capacity is reached. Return orders that are not packed, trucks that are not filled and booked orders.
    # Sort orders by volume
    orders = orders.sort_values('Total Volume',ascending=False)
    min_v = orders['Volume per tyre'].min() + 0.001 # minimum volume needed for one tire
    # Start filling from cheapest truck
    trucks = trucks.sort_values('Price')
    cur_o = 0
    # Verovatno treba imati i podatak o narudzbinama u kamionu, isto tako i u tome kojim sve kamionima ide neka veca narudzbina, i da li je cela poslata itd.
    booked_t = []#{'truck':h_id, 'status':'part-full','orders':[{'order':o_id, 'quantity':o_qty, 'volume':o_vol}]},'remaining_v':0}]
    booked_o = []#{'order':o_id, 'truck':h_id, 'volume':o_vol, 'quantity':o_qty}]
    split_o = None #[{'order':o_id, 'remaining_v':o_vol}]
    for i,r in trucks.iterrows():
        if len(booked_t)>=wh_cap:
            break
        t_vol = r['Effective Volume'] # truck capacity
        h_id = r['Haulier ID'] # haulier
        t = {'truck':h_id, 'status':'empty','orders':[]}
        added_os = [o.key() for o in t['orders']]
        vol = 0
        # New truck, check if there is a split order
        if split_o:
            sp_o_id = split_o['order']
            sp_t_v = split_o['tire_v']
            sp_vol=split_o['remaining_v']
            sp_qty=split_o['remaining_q']
            if sp_vol==t_vol:
                # the remaining volume is exactly the same as the truck volume
                if sp_o_id not in added_os:
                    t['orders'].append({'order':sp_o_id, 'quantity':sp_qty, 'volume':sp_vol})
                t['status']='full'
                t['remaining_v']=0
                if date:
                    t['date'] = date
                booked_t.append(t)
                wh_cap-=1
                if sp_o_id not in [o['order'] for o in booked_o]:
                    booked_o.append({'order':sp_o_id, 'truck':h_id,
                                'volume':vol, 'quantity':sp_qty})
                else:
                    for b_o in booked_o:
                        if b_o['order']==sp_o_id:
                            b_o['volume']+=sp_vol
                            b_o['quantity']+=sp_qty
                cur_o+=1 # move to the next order
                split_o = None
                continue
            elif sp_vol>t_vol:
                # the remaining volume is larger than the truck volume
                # This truck will be filled with part of this order
                # How many tires can be added to the truck
                t_fit_qty = t_vol//(sp_t_v * volume_scale)
                t_fit_vol = t_fit_qty*sp_t_v * volume_scale
                t_rem_vol = t_vol-t_fit_vol
                if sp_o_id not in [o['order'] for o in booked_o]:
                    booked_o.append({'order':sp_o_id, 'truck':h_id,
                        'volume':t_fit_vol, 'quantity':t_fit_qty})
                else:
                    for b_o in booked_o:
                        if b_o['order']==sp_o_id:
                            b_o['volume']+=t_fit_vol
                            b_o['quantity']+=t_fit_qty
                t['orders'].append({'order':sp_o_id, 'quantity':t_fit_qty,  'volume':t_fit_vol})
                if t_rem_vol>min_v:
                    t['status']='part-full'
                    t['remaining_v']=t_rem_vol
                if date:
                    t['date'] = date
                booked_t.append(t)
                wh_cap-=1
                split_o['remaining_v'] -= t_fit_vol
                split_o['remaining_q'] -= t_fit_qty 
                continue
            else:
                # the remaining split o volume is smaller than the truck volume
                t['orders'].append({'order':sp_o_id, 'quantity':sp_qty, 'volume':sp_vol})
                t['status']='part-full'
                vol+=sp_vol
                cur_o+=1              
        while vol<t_vol and cur_o<len(orders):
            # take order
            o_id = orders.iloc[cur_o].name #['Order Number']
            o_vol = orders.iloc[cur_o]['Total Volume']
            o_qty = orders.iloc[cur_o]['Balance Order Quantity']
            o_t_v = orders.iloc[cur_o]['Volume per tyre']
            if vol+o_vol<=t_vol:
                # oder fits in the truck completely
                t['orders'].append({'order':o_id, 'quantity':o_qty, 'volume':o_vol})
                t['status']='part-full'
                vol+=o_vol
                cur_o+=1
            else:
                t_rem_vol = t_vol-vol
                t_fit_qty = t_rem_vol//(o_t_v * volume_scale)
                t_fit_vol = t_fit_qty*o_t_v * volume_scale
                if t_rem_vol==0 or t_fit_qty==0:
                    # the last order was exactly the same as the remaining volume or new order is too big (not even 1 tire fits)
                    t['status']='full'
                    split_o = None
                    break
                # this order should be split
                # Or first try to find a smaller order that can fit?
                # Maybe this is better, analyse left volume first, and if there are no orders that can fit, split the order
                # BuT this is all for one customer,so maybe it does not matter
                t['orders'].append({'order':o_id, 'quantity':t_fit_qty, 'volume':t_fit_vol})
                t['status']='full'
                split_o = {'order':o_id, 'remaining_v':o_vol-t_fit_vol, 'tire_v':o_t_v, 'remaining_q':o_qty-t_fit_qty}
                break
        if t['status']=='full':
            if date:
                t['date'] = date
            booked_t.append(t)
            wh_cap-=1
            booked_o_ids = [bo['order'] for bo in booked_o]
            for t_o in t['orders']:
                # for each order in the truck
                t_o_id = t_o['order']
                t_o_q = t_o['quantity']
                t_o_v = t_o['volume']
                if t_o_id not in booked_o_ids:
                    booked_o.append({'order':t_o_id, 'truck':h_id,
                        'volume':t_o_v, 'quantity':t_o_q})
                else:
                    for b_o in booked_o:
                        if b_o['order']==t_o_id:
                            b_o['volume']+=t_o_v
                            b_o['quantity']+=t_o_q
        else:
            # this truck is not full, which means that there are no more orders
            # Maybe here check how much space is left, maybe it's ok to leave it like this
            break

    # Return everything booked, everything left, and split order if existing, ad wh capacity left
    bk_o_ids = [bo['order'] for bo in booked_o]
    rem_o = orders[~orders.index.isin(bk_o_ids)] # only full ord that are not booked
    if split_o:
        add_o = orders.loc[split_o['order']]
        add_o['Total Volume'] = split_o['remaining_v']
        add_o['Balance Order Quantity'] = split_o['remaining_q']
        rem_o = pd.concat([rem_o,add_o]) # add split order to the remaining orders
    #rem_t = trucks[~trucks['contractNumber'].isin([bt['truck'] for bt in booked_t])]
    return booked_t, booked_o, rem_o, wh_cap