import sys

import pandas as pd

sys.path.append('/home/ogi/01_Projekti/ZEUS/code')
from zeus_utils import add_ord_loc, fill_trucks_customer



basedir = '/home/ogi/01_Projekti/ZEUS/Podaci'
cust_f = f'{basedir}/Apollo - ship_to_export.csv'
orders_f = f'{basedir}/Order Wallet + Shipping Vol Wt 20240912.xlsx'
wh_f = f'{basedir}/Apollo Warehouse Address V2 20240729_GILAB.xlsx'
vh_f = f'{basedir}/Zeus - Apollo - list of trailer types 20240902.xlsx'
vh = pd.read_excel(vh_f)
vh_type_mapping = {'STANDARD_TRAILER':'tautliner-13-6m-44ft',
                   'XL_TRAILER':'high-cube-or-tall-triler',
                    'BOX_TRAILER':'box-13-6m-44ft', 
                    'MEGA_TRAILER':'mega-trailer-13-6m-44ft'}
vh['vehicleType'] = vh['Apollo (Equip type ID)'].map(vh_type_mapping)
# use only some trailers?
orders = pd.read_excel(orders_f)
orders['Delivery Date'] = pd.to_datetime(orders['Delivery Date'])
orders['Material Availability Date'] = orders['Delivery Date'] - pd.Timedelta(days=14)
orders['MAD'] = orders['Material Availability Date']
orders['DD'] = orders['Delivery Date']
orders['Shipping Point'] = orders['Shipping Point'].astype(str)
orders['Ship to Party'] = orders['Ship to Party'].astype(str)
cust = pd.read_csv(cust_f, header=None)
cust.columns = ['Ship to Party','party_name','party_address','party_city','party_zipcode','party_country']
cust['Ship to Party'] = cust['Ship to Party'].astype(str)
cust.set_index('Ship to Party',inplace=True)
contracts = pd.read_csv(f'{basedir}/contracts.csv')
contracts = contracts[contracts['price'] > 0]
hauls = contracts.copy(deep=True)
hauls['available'] = True
hauls['availableFrom'] = None
# Add vehicle type from apollo
hauls = pd.merge(hauls,vh,on='vehicleType')
whs = pd.read_excel(wh_f)
wh_capacities = {'5HU2': 25, '5HU5': 25} # HARD CODED FOR NOW
wh_ids = ['5HU2','5HU5']
volume_scale = 0.8  # If tires are packed laced, TEMPORARY APPROX
min_qty = 1000 # Drop customers below certain treshold, TEMPORARY APPROX
abs_min_qty = 400 # Drop customers below certain treshold, TEMPORARY APPROX
additionalKmRate = 1.15
freeKmAmount = 75
fuelSurchargePercent = 6.14

def wh_pack_orders(wh_id, orders):
    # Work per warehouse, maybe later see if we can combine orders from multiple warehouses into one truck (if they are close enough)
    # wh_id = '5HU5'
    ord = orders
    wh = whs[whs['Shipping point \n(Primary key for mapping of loads from OTM)'] == wh_id]
    wh_ord = ord[ord['Shipping Point']==wh_id]
    # wh_ord['Balance Order Quantity'].sum()
    wh_city = wh['City'].values[0]
    wh_dep_t = 12 # departure time, HARD CODED FOR NOW
    wh_haul = hauls[(hauls['cityFrom'] == wh_city)] # TODO: Get only hauliers that are currently available, keep track of departure and return dates
    # Get order availability dates
    dates = wh_ord['MAD'].dt.date.unique()
    dates.sort()
    # Gledaju se prvo narudzbine velike za istu musteriju (>1000), pa onda preostale u istom gradu, pa onda ako ne moze u roku od 7 dana on MADa, onda moze i ista zemlja. Ako nista od toga, i ako je narudzbina manja od 400, onda se brise.
    wh_haul['availableFrom'] = dates[0]
    b_ord = [] # Booked orders
    b_truck = [] # Booked trucks
    ord_backlog = []
    # Traverse dates
    for d in dates:
        print(d)
        # See if any of the hauliers returned
        wh_haul['available'] = wh_haul['availableFrom'] <= d # TODO: Check
        wh_haul_a = wh_haul[wh_haul['available']]
        wh_cap = wh_capacities[wh_id] # Warehouse capacity, each day new
        #d = dates[0]
        d_ord = wh_ord[wh_ord['MAD'].dt.date == d]
        # TODO: Check this calculation, find better way
        d_ord.loc[:,'Gross total weight'] = d_ord['Balance Order Quantity']*d_ord['Net Weight']
        d_ord.loc[:,'Total Volume'] = d_ord['Balance Order Quantity']*d_ord['Volume per tyre']*volume_scale
        # Doda se odmah i lokacija, posle se iz d_ord samo uzimaju podaci, na osnovu razlicitih grupisanja
        d_ord = add_ord_loc(d_ord,cust)
        d_ord['loc'] = d_ord['party_country']+'-'+d_ord['party_zipcode'].str[:2]
        #--------# Group by customer
        d_ord_sum = d_ord.groupby(['Ship to Party']).agg({'Gross total weight':sum,'Total Volume':sum,'Balance Order Quantity':sum}) # ,'SKU Code']
        # Big enough orders for the day, that can fill at least one truck
        d_ord_xl = d_ord_sum[d_ord_sum['Balance Order Quantity']>=min_qty]
        # Other orders
        d_ord_sml = d_ord_sum[d_ord_sum['Balance Order Quantity']<min_qty]
        ord_sml = d_ord[d_ord['Ship to Party'].isin(d_ord_sml.index)]
        orders_left = []
        #################################
        if not d_ord_xl.empty:
            # there are big orders for one or more customers
            d_ord_xl = d_ord_xl.sort_values('Total Volume',ascending=False)
            # For each customer book trucks
            for c in d_ord_xl.index.get_level_values(0).unique():
                if wh_cap==0:
                    # If no more wh capacity
                    print('No more wh capacity')
                    not_more_capacity = True #TODO: Check
                    orders_left.append(c)
                    continue
                # Get today orders for this customer
                d_ord_c = d_ord[d_ord['Ship to Party']==c]
                # Pull wh_hauliers for this customer and start filling
                haul_loc=d_ord_c.iloc[0]['loc']
                c_haul = wh_haul[wh_haul['regionCodeTo']==haul_loc]
                if c_haul.empty:
                    # If no hauliers for this customer, add to backlog
                    print('No hauliers for this customer')
                    orders_left.append(c)
                    continue
                c_haul = c_haul.sort_values('price')
                # Book orders, and return booked orders and trucks, and left orders and trucks, split orders and warehouse capacity
                d_ord_t, d_ord_b, d_ord_rem_o, wh_cap = fill_trucks_customer(d_ord_c, c_haul, wh_cap)
                b_ord+=d_ord_b
                b_truck+=d_ord_t
                # Remove booked hauliers from available hauliers
                booked_h = [t['truck'] for t in d_ord_t]
                wh_haul_a = wh_haul_a[~wh_haul_a['contractNumber'].isin(booked_h)]
                wh_haul.loc[wh_haul['contractNumber'].isin(booked_h),'available'] = False
                wh_haul.loc[wh_haul['contractNumber'].isin(booked_h),'availableFrom'] = d + pd.Timedelta(days=5)
                print(wh_haul['availableFrom'])
                # TODO: Add return date for each haulier
                # If there are unbooked orders, add them to backlog
                if d_ord_rem_o:
                    orders_left.append(d_ord_rem_o)
        if wh_cap==0:
            # If no more wh capacity
            print('No more wh capacity')
            ord_backlog = ord_backlog + orders_left
            ord_backlog.append(ord_sml)
            continue
        #--------# Merge xl ord leftovers and small orders from c grouping
        d_ord_left = pd.concat(orders_left + [ord_sml])
        orders_left = []
        # Group by country and city to try and fill truck for city
        d_ord_left_sum = d_ord_left.groupby(['loc']).agg({'Gross total weight':sum,'Total Volume':sum,'Balance Order Quantity':sum})
        d_ord_left_sum = d_ord_left_sum.sort_values('Balance Order Quantity',ascending=False)
        d_ord_left_sum_xl= d_ord_left_sum[d_ord_left_sum['Balance Order Quantity']>=min_qty]
        d_ord_left_sum_sml= d_ord_left_sum[d_ord_left_sum['Balance Order Quantity']<min_qty]
        d_ord_left_small = d_ord_left[d_ord_left['loc'].isin(d_ord_left_sum_sml.index)]
        if d_ord_left_sum_xl.empty:
            # If the largest order is smaller than one truck, add to backlog
            orders_left.append(d_ord_left)
        else:
            # For each location book trucks
            for l in d_ord_left_sum_xl.index.get_level_values(0).unique():
                # Separate orders for this location
                d_ord_left_xl_l = d_ord_left[d_ord_left['loc']==l]
                if wh_cap==0:
                    print('No more wh capacity')
                    # If no more wh capacity
                    orders_left.append(d_ord_left_xl_l)
                    continue
                # Pull wh_hauliers for this location by desitnation, and start filling trucks from lowest price
                l_haul = wh_haul[wh_haul['regionCodeTo']==l]
                if l_haul.empty:
                    # If no hauliers for this location, add to backlog
                    orders_left.append(d_ord_left_xl_l)
                    continue
                l_haul = l_haul.sort_values('price')
                # Book orders, and return booked orders and trucks, and left orders and trucks, split orders and warehouse capacity
                d_ord_t, d_ord_b, d_ord_rem_o, wh_cap = fill_trucks_customer(d_ord_left_xl_l, l_haul, wh_cap)
                b_ord+=d_ord_b
                b_truck+=d_ord_t
                # Remove booked hauliers from available hauliers
                booked_h = [t['truck'] for t in d_ord_t]
                wh_haul_a = wh_haul_a[~wh_haul_a['contractNumber'].isin(booked_h)]
                wh_haul.loc[wh_haul['contractNumber'].isin(booked_h),'available'] = False
                wh_haul.loc[wh_haul['contractNumber'].isin(booked_h),'availableFrom'] = d + pd.Timedelta(days=5)
                # If there are split orders and unbooked orders, add them to backlog
                if d_ord_rem_o:
                    orders_left.append(d_ord_rem_o)
            #------# Merge xl ord leftovers and small orders from loc grouping
            orders_left = orders_left + [d_ord_left_small]
        if wh_cap==0:
            # If no more wh capacity
            print('No more wh capacity')
            ord_backlog = ord_backlog + orders_left
            continue
        #--------# After all possible is booked for this day, try with backlog
        ord_backlog_new = []
        ord_old = pd.concat(ord_backlog + orders_left)
        ord_old.reset_index(inplace=True, drop=True)
        # ord_old['Balance Order Quantity'].sum()
        # Group by country and city to try and fill truck for city
        ord_old_sum = ord_old.groupby(['loc']).agg({'Gross total weight':sum,'Total Volume':sum,'Balance Order Quantity':sum})
        ord_old_sum= ord_old_sum.sort_values('Total Volume',ascending=False)
        #################### OGI##########
        ########## OVDE STAVI ISTO KAO GORE, ONO STO OSTANE IDE PO COUTRY
        # Continue with the same logic as above, for full trucks or >900
        ord_old_sum_xl= ord_old_sum[ord_old_sum['Balance Order Quantity']>=min_qty]
        ord_old_sum_sml= ord_old_sum[ord_old_sum['Balance Order Quantity']<min_qty]
        ord_old_small = ord_old[ord_old['loc'].isin(ord_old_sum_sml.index)]
        if not ord_old_sum_xl.empty:
            for l in ord_old_sum_xl.index:
                if wh_cap==0:
                    # If no more wh capacity
                    ord_backlog_new.append(ord_old_sum_xl.loc[l])
                    continue
                # Separate summed orders for this location
                ord_old_l = ord_old[ord_old['loc']==l]
                # Pull wh_hauliers for this location by desitnation, and start filling trucks from lowest price
                l_haul = wh_haul_a[wh_haul_a['regionCodeTo']==l]
                if l_haul.empty:
                    # If no hauliers for this location, add to backlog
                    ord_backlog_new.append(ord_old_l)
                    continue
                l_haul = l_haul.sort_values('price')
                # Book orders, and return booked orders and trucks, and left orders and trucks, split orders and warehouse capacity
                d_ord_t, d_ord_b, d_ord_rem_o, wh_cap = fill_trucks_customer(ord_old_l, l_haul, wh_cap)
                b_ord+=d_ord_b
                b_truck+=d_ord_t
                # Remove booked hauliers from available hauliers
                booked_h = [t['truck'] for t in d_ord_t]
                wh_haul_a = wh_haul_a[~wh_haul_a['contractNumber'].isin(booked_h)]
                wh_haul.loc[wh_haul['contractNumber'].isin(booked_h),'available'] = False
                wh_haul.loc[wh_haul['contractNumber'].isin(booked_h),'availableFrom'] = d + pd.Timedelta(days=5)
                print(wh_haul['availableFrom'])
                # If there are split orders and unbooked orders, add them to backlog
                if d_ord_rem_o:
                    ord_backlog_new.append(d_ord_rem_o)
        # Now all that is left are small orders and ord_backlog_new
        ord_backlog = ord_backlog_new + [ord_old_small]
        ord_cuopt = pd.concat(ord_backlog)
        # After all that is possible is booked for this day, if there is still wh capacity, agg by country and try cuOpt
        # Sanity check
        wh_ord['Balance Order Quantity'].sum()
        ord_cuopt['Balance Order Quantity'].sum()
        ###############################################
        # for o in ord_backlog:
        #         if wh_cap==0:
        #             # If no more wh capacity
        #             ord_backlog.append(o)
        #             continue
        #         # Separate summed orders for this location
        #         o = o.sort_values('Total Volume',ascending=False)
        #         # Pull wh_hauliers for this location by desitnation, and start filling trucks from lowest price
        #         o_haul = wh_haul[wh_haul['regionCodeTo']==l]
        #         if o_haul.empty:
        #             # If no hauliers for this location, add to backlog
        #             ord_backlog.append(o)
        #             continue
        #         o_haul = o_haul.sort_values('price')
        #         # Add truck data
        #         o_haul = pd.merge(o_haul,vh,on='vehicleType')
        #         # Book orders, and return booked orders and trucks, and left orders and trucks, split orders and warehouse capacity
        #         d_ord_t, d_ord_b, d_ord_rem_o, wh_cap = fill_trucks_customer(o, o_haul, wh_cap)
        #         b_ord+=d_ord_b
        #         b_truck+=d_ord_t
        #         # Remove booked hauliers from available hauliers
        #         booked_h = [t['truck'] for t in d_ord_t]
        #         wh_haul_a = wh_haul_a.loc[~wh_haul_a['contractNumber'].isin(booked_h)]
        #         wh_haul.loc[booked_h,'available'] = False
        #         wh_haul.loc[booked_h,'availableFrom'] = d + pd.Timedelta(days=5)
        #         # If there are split orders and unbooked orders, add them to backlog
        #         if d_ord_rem_o:
        #             ord_backlog.append(d_ord_rem_o)

        
        

   