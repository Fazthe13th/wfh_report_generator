from database import connect_database
from datetime import datetime


class ProductivityReport:
    @classmethod
    def get_unsynced_activity_uuid(cls):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            unsynced_uuids = """select activity_uuid from vpn_user_activity where activity_uuid NOT IN
                            (select activity_hist_uuid from synced_activity_hist)"""
            cursor.execute(unsynced_uuids)
            uuids = cursor.fetchall()
            cursor.close()
            vpn_log.close()
            return uuids

        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch unsynced uuids: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def select_activity(cls, uuid):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_activity = """SELECT user_id,username,host,url_access_time FROM vpn_log.vpn_user_activity where activity_uuid  = %s"""
            cursor.execute(select_activity, (str(uuid),))
            activity_info = cursor.fetchone()
            activity_info_dict = {
                'vpn_user_id': activity_info[0],
                'vpn_user_name': activity_info[1],
                'host': activity_info[2],
                'date': activity_info[3].date()
            }
            cursor.close()
            vpn_log.close()
            return activity_info_dict

        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch activity info: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def check_if_in_bw_list(cls, ldap_user_name):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_bw_list = """SELECT user_id,ldap_user_name,host_id,flag FROM black_list_white_lists 
            where ldap_user_name = %s and deleted_at is null or deleted_at = 0"""
            cursor.execute(select_bw_list, (str(ldap_user_name),))
            bw_info = cursor.fetchall()
            cursor.close()
            vpn_log.close()
            return bw_info if bw_info else None

        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Check in bw failed: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def black_white_host_array(cls, bw):
        black_list = []
        white_list = []
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        for row in bw:
            user_id = row[0]
            ldap_user_name = row[1]
            host_id = row[2]
            flag = row[3]
            try:
                select_host_label = """SELECT label FROM vpn_log.hosts where id = %s"""
                cursor.execute(select_host_label, (int(host_id),))
                host_label_tup = cursor.fetchone()
                host_label = host_label_tup[0]
            except Exception as e:
                print('Creating black list & white lisy failed: ' + str(e))
                return {
                    'error': '1111',
                    'message': 'Can not fetch. SQL error'
                }
            if flag == 0:
                white_list.append(host_label)
            if flag == 1:
                black_list.append(host_label)
        return black_list, white_list

    @classmethod
    def check_ldap_user_name_date(cls, ldap_user_name, date):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_productivity = """SELECT user_id,ldap_user_name,vpn_user_id,productive_count,unproductive_count,
            unclassified_count,date FROM vpn_log.productivity_per_day where ldap_user_name = %s and date = %s"""
            cursor.execute(select_productivity, (str(ldap_user_name), date))
            productivity_row = cursor.fetchone()
            select_web_user_id = """SELECT id FROM vpn_log.users where ldap_user_name = %s"""
            cursor.execute(select_web_user_id, (str(ldap_user_name),))
            web_user_id_raw = cursor.fetchone()
            web_user_id = None
            if web_user_id_raw:
                web_user_id = web_user_id_raw[0]
            cursor.close()
            vpn_log.close()
            return productivity_row, web_user_id
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Check productivity exixts failed: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def insert_in_productivity(cls, *data):
        web_user_id, vpn_user_name, vpn_user_id, black, white, unclass, date = data
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            insert_productivity = """INSERT INTO vpn_log.productivity_per_day 
            (
            user_id,
            ldap_user_name,
            vpn_user_id,
            productive_count,
            unproductive_count,
            unclassified_count,
            date) 
            VALUES 
            (%s, %s,%s, %s, %s,%s, %s)"""
            val = (web_user_id, vpn_user_name, vpn_user_id,
                   white, black, unclass, date)
            cursor.execute(insert_productivity, val)
            vpn_log.commit()
            print('Added user productivity info!')
            cursor.close()
            vpn_log.close()
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('User productivity not inserted.Error happened: ' + str(e))

    @classmethod
    def update_in_productivity_report(cls, *data):
        web_user_id, ldap_user_name, vpn_user_id, new_productive, new_unproductive, new_unclass, date = data
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            cursor.execute("""
            UPDATE vpn_log.productivity_per_day
            SET user_id=%s,productive_count=%s,unproductive_count=%s,unclassified_count=%s
            WHERE ldap_user_name=%s and date=%s
                """, (web_user_id, new_productive, new_unproductive, new_unclass, ldap_user_name, date))
            vpn_log.commit()
            print('Updated user productivity info in users!')
            cursor.close()
            vpn_log.close()
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Update failed during productive info. Error happened: ' + str(e))

    @classmethod
    def store_sync_activity_uuid(cls, uuid):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            insert_uuid = """INSERT INTO vpn_log.synced_activity_hist 
            (
            activity_hist_uuid) 
            VALUES 
            (%s)"""
            val = (str(uuid),)
            cursor.execute(insert_uuid, val)
            vpn_log.commit()
            print('Added activity uuid to synced table!')
            cursor.close()
            vpn_log.close()
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Activity uuid sync was not successful.Error happened: ' + str(e))

    def productivity_report_generate(self):
        unsynced_uuids = self.get_unsynced_activity_uuid()
        for row in unsynced_uuids:
            black = 0
            white = 0
            unclass = 0
            uuid = row[0]
            # select activity
            activity_info_dict = self.select_activity(uuid)
            # check vpn_user_name in blacklist_whitelist
            check_in_bw = self.check_if_in_bw_list(
                activity_info_dict['vpn_user_name'])
            # if in bw than create array of b list w list (look for deleted at column)
            if check_in_bw:
                black_list, white_list = self.black_white_host_array(
                    check_in_bw)
            # black or wite list host
            host = str(activity_info_dict['host'])
            if host in black_list:
                black = 1
            elif host in white_list:
                white = 1
            else:
                unclass = 1
            # check the report table
            productivity_report_row, web_user_id = self.check_ldap_user_name_date(
                activity_info_dict['vpn_user_name'], activity_info_dict['date'])

            # if not than insert doin if esle using b list w list
            if not productivity_report_row:
                self.insert_in_productivity(
                    web_user_id, activity_info_dict['vpn_user_name'], activity_info_dict['vpn_user_id'], black, white, unclass, activity_info_dict['date'])
            # or update and add count
            else:
                user_id, ldap_user_name, vpn_user_id, productive_count, unproductive_count, unclassified_count, date = productivity_report_row
                new_productive = int(productive_count) + white
                new_unproductive = int(unproductive_count) + black
                new_unclass = int(unclassified_count) + unclass
                self.update_in_productivity_report(
                    web_user_id, ldap_user_name, vpn_user_id, new_productive, new_unproductive, new_unclass, date)
            self.store_sync_activity_uuid(uuid)
