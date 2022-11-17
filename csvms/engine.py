"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""

class Engine():
    """Class used to implement bootcamp tasks"""
    def execute(self, sql:str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        #TODO Implement your SQL engine

        def create_table(qry):
            from mo_sql_parsing import parse
            from csvms.table import Table
            import json

            text_values = ['str', 'text']
            int_values = ['int', 'integer']
            floating_values = ['float', 'double']
            
            
            parsed_qry = parse(qry)
            columns = list(parsed_qry['create table']['columns'])
            
            cols = {}
            for col in columns:
                extraction = "{"+'"'+ str(col['name']) +'"'+ ":" + '"' + str(list(col['type'].keys())[0]) + '"' + "}"
                extraction = json.loads(extraction)

                for key, value in extraction.items():
                
                    if value.lower() in text_values:
                        extraction[key] = str

                    elif value.lower() in int_values:
                        extraction[key] = int

                    elif value.lower() in floating_values:
                        extraction[key] = float

                cols.update(extraction)

            table_name = parsed_qry["create table"]["name"]

            Table(name = table_name, columns = cols).save() 

        def insert_values(qry):
            from mo_sql_parsing import parse
            from csvms.table import Table

            parsed_qry = parse(qry)
            data_list = parsed_qry['query']['select']

            dt_list = list()
            for data in data_list:
                if isinstance(data['value'], dict):
                    dt_list.append(data['value']['literal'])
                else:
                    dt_list.append(data['value'])
            
            tbl = Table(parsed_qry['insert'])

            tbl.append(*dt_list)
            tbl.save()

        def update_table(qry):
            from mo_sql_parsing import parse
            from csvms.table import Table

            parsed_qry = parse(qry)

            tbl = Table(parsed_qry['update'])

            if 'eq' in parsed_qry['where']:
                for r in range(0, len(tbl)):
                    if isinstance(parsed_qry['set'][list(parsed_qry['set'].keys())[0]], dict):
                        if isinstance(parsed_qry['where']['eq'][1], dict):
                            if tbl[r][parsed_qry['where']['eq'][0]] == parsed_qry['where']['eq'][1]['literal']:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]['literal']

                                tbl[r] = tuple(row.values())
                                tbl.save()
                        else:
                            if tbl[r][parsed_qry['where']['eq'][0]] == parsed_qry['where']['eq'][1]:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]['literal']

                                tbl[r] = tuple(row.values())
                                tbl.save()
                    else:
                        if isinstance(parsed_qry['where']['eq'][1], dict):
                            if tbl[r][parsed_qry['where']['eq'][0]] == parsed_qry['where']['eq'][1]['literal']:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]

                                tbl[r] = tuple(row.values())
                                tbl.save()
                        else:
                            if tbl[r][parsed_qry['where']['eq'][0]] == parsed_qry['where']['eq'][1]:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]

                                tbl[r] = tuple(row.values())
                                tbl.save()

                                

            elif 'neq' in parsed_qry['where']:
                for r in range(0, len(tbl)):
                    if isinstance(parsed_qry['set'][list(parsed_qry['set'].keys())[0]], dict):
                        if isinstance(parsed_qry['where']['eq'][1], dict):
                            if tbl[r][parsed_qry['where']['eq'][0]] != parsed_qry['where']['eq'][1]['literal']:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]['literal']

                                tbl[r] = tuple(row.values())
                                tbl.save()
                        else:
                            if tbl[r][parsed_qry['where']['eq'][0]] != parsed_qry['where']['eq'][1]:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]['literal']

                                tbl[r] = tuple(row.values())
                                tbl.save()
                    else:
                        if isinstance(parsed_qry['where']['eq'][1], dict):
                            if tbl[r][parsed_qry['where']['eq'][0]] != parsed_qry['where']['eq'][1]['literal']:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]

                                tbl[r] = tuple(row.values())
                                tbl.save()
                        else:
                            if tbl[r][parsed_qry['where']['eq'][0]] != parsed_qry['where']['eq'][1]:
                                row = tbl[r]

                                row[list(parsed_qry['set'].keys())[0]] = parsed_qry['set'][list(parsed_qry['set'].keys())[0]]

                                tbl[r] = tuple(row.values())
                                tbl.save()

        def delete_row(qry):
            from mo_sql_parsing import parse
            from csvms.table import Table

            parsed_qry = parse(qry)

            tbl = Table(parsed_qry['delete'])

            if 'eq' in parsed_qry['where']:
                for r in reversed(range(0, len(tbl))):
                    if tbl[r][parsed_qry['where']['eq'][0]] == parsed_qry['where']['eq'][1]['literal']:
                        del tbl[r]
                        tbl.save()

            elif 'neq' in parsed_qry['where']:
                for r in reversed(range(0, len(tbl))):
                    if tbl[r][parsed_qry['where']['neq'][0]] != parsed_qry['where']['neq'][1]['literal']:
                        del tbl[r]
                        tbl.save()

        def select(qry):
            from mo_sql_parsing import parse
            from csvms.table import Table

            parsed_qry = parse(qry)

            print(parsed_qry)

            if isinstance(parsed_qry['from'], str):
                tbl_name = Table(parsed_qry['from'])

                if isinstance(parsed_qry['select'], dict):
                    slc = [parsed_qry['select']]
                    #slc = [col for col in list(parsed_qry['select'].values())]
                else:
                    slc = parsed_qry['select']

                tbl = tbl_name.π(slc)
                print(tbl)

            elif isinstance(parsed_qry['from'], list):
                from_com = parsed_qry['from']
                
                counter = 0
                for com in from_com:
                    if 'value' in com:
                        if counter >= 1:
                            tbl_2 = Table(com['value'])
                            tbl_2 = tbl_2.ρ(com['name'])
                            condition = parsed_qry['where']
                            tbl = tbl.ᐅᐊ(tbl_2, condition)
                        else:
                            tbl = Table(com['value'])
                            tbl = tbl.ρ(com['name'])
                        counter += 1
                    elif 'inner join' in com:
                        tbl_2 = Table(com['inner join']['value'])
                        tbl_2 = tbl_2.ρ(com['inner join']['name'])
                        condition = com['on']
                        tbl = tbl.ᐅᐊ(tbl_2, condition)
                    elif 'right join' in com:
                        tbl_2 = Table(com['right join']['value'])
                        tbl_2 = tbl_2.ρ(com['right join']['name'])
                        condition = com['on']
                        tbl = tbl.ᐅᗏ(tbl_2, condition)
                    elif 'left join' in com:
                        tbl_2 = Table(com['left join']['value'])
                        tbl_2 = tbl_2.ρ(com['left join']['name'])
                        condition = com['on']
                        tbl = tbl.ᐅᗏ(tbl_2, condition)
                    #add mais elifs para os outros joins

                #cant find all cols na issue #23
                if isinstance(parsed_qry['select'], dict):
                    slc = [parsed_qry['select']]
                    #slc = [col for col in list(parsed_qry['select'].values())]
                else:
                    slc = parsed_qry['select']
        

                prj = []
                for i in slc:
                    if isinstance(i['value'], dict):
                        prj.append(i)
                        slc.remove(i)
                        
                if 'where' in parsed_qry:
                    tbl = tbl.σ(parsed_qry['where']).π(slc)
                    if len(prj) >= 1:
                        for j in prj:
                            tbl = tbl.Π(j['value'], alias=j['name'])
                else:
                    print([{'value': f'{tbl.name}.tp_fruta', 'name': 'tipos sem frutas'}])
                    tbl = tbl.π(slc)
                    if len(prj) >= 1:
                        for j in prj:
                            tbl = tbl.Π(j['value'], alias=j['name'])


                print(tbl)


        def run_query(qry):
            if ";" in qry:
                qry = qry.split(";")
                del qry[-1]

                for q in qry:
                    if 'create table' in q.lower(): create_table(q)
                    elif 'insert into' in q.lower(): insert_values(q)
                    elif 'update' in q.lower(): update_table(q)
                    elif 'delete' in q.lower(): delete_row(q)
                    elif 'select' in q.lower(): select(q)
                    
            else:
                if 'create table' in qry.lower(): create_table(qry)
                elif 'insert into' in qry.lower(): insert_values(qry)
                elif 'update' in qry.lower(): update_table(qry)
                elif 'delete' in qry.lower(): delete_row(qry)
                elif 'select' in qry.lower(): select(qry)
                    

        run_query(sql)

        
        #raise NotImplementedError
