from database import connect_database
from datetime import datetime


class IdleBusyReport:

    @classmethod
    def unsynced_access_uuids(cls):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            unsynced_uuids = """select access_hist_uuid from vpn_user_access_history where access_hist_uuid NOT IN
                            (select access_hist_uuid from synced_access_hist)"""
            cursor.execute(unsynced_uuids)
            uuids = cursor.fetchall()
            cursor.close()
            vpn_log.close()
            return uuids

        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch unsynced access uuids: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def access_hist_data_dict(cls, uuid):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_access = """SELECT user_id,login_time,logout_time FROM vpn_log.vpn_user_access_history where access_hist_uuid = %s and DATE(login_time) != CURDATE() and (logout_time IS NOT NULL or logout_time != 0)"""
            cursor.execute(select_access, (str(uuid),))
            access_info = cursor.fetchone()
            access_info_dict = None
            if access_info:
                select_ldap_username = """SELECT user_name FROM vpn_log.vpn_users where user_id = %s"""
                cursor.execute(select_ldap_username, (int(access_info[0]),))
                user_info = cursor.fetchone()
                ldap_user_name = user_info[0]
                select_web_id = """SELECT id FROM vpn_log.users where ldap_user_name = %s"""
                cursor.execute(select_web_id, (str(ldap_user_name),))
                web_user_info = cursor.fetchone()
                web_user_id = None
                if web_user_info:
                    web_user_id = web_user_info[0]
                access_info_dict = {
                    'web_user_id': web_user_id,
                    'ldap_user_name': ldap_user_name,
                    'vpn_user_id': access_info[0],
                    'login_time': access_info[1],
                    'logout_time': access_info[2]
                }
            cursor.close()
            vpn_log.close()
            return access_info_dict

        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch activity info: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def check_if_in_time_sheet(cls, vpn_user_name):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_date_time = """SELECT start_datetime,end_datetime,id FROM vpn_log.time_sheets where ldap_user_name = %s and deleted_at is null order by id desc limit 1"""
            cursor.execute(select_date_time, (str(vpn_user_name),))
            date_time_info = cursor.fetchone()
            cursor.close()
            vpn_log.close()
            return date_time_info

        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch activity info: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def find_week_end(cls, time_sheet_id):
        day_name_map = {1: 'SATURDAY',
                        2: 'SUNDAY',
                        3: 'MONDAY',
                        4: 'TUESDAY',
                        5: 'WEDNESDAY',
                        6: 'THURSDAY',
                        7: 'FRIDAY'}

        session_time = 0
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_weekends = """SELECT weekends FROM vpn_log.time_sheet_weekends where time_sheet_id = %s and deleted_at is NULL"""
            cursor.execute(select_weekends, (int(time_sheet_id),))
            weekends = cursor.fetchone()
            weekends_array = []
            if weekends:
                for row in weekends:
                    day_name = day_name_map[row]
                    weekends_array.append(day_name.lower())
            cursor.close()
            vpn_log.close()
            return weekends_array
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch activity info: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def check_record_exists_in_report(cls, vpn_user_name, date):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            select_report = """SELECT busy_in_hour,idle_in_hour FROM vpn_log.idle_busy_per_day where ldap_user_name = %s and date = %s"""
            cursor.execute(select_report, (str(vpn_user_name), date))
            record = cursor.fetchone()
            cursor.close()
            vpn_log.close()
            return record
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Can not fetch activity info: ' + str(e))
            return {
                'error': '1111',
                'message': 'Can not fetch. SQL error'
            }

    @classmethod
    def insert_in_report_table(cls, *data):
        web_user_id, ldap_user_name, vpn_user_id, session_time_in_hour, idle_time_in_hour, login_date, day_type = data
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            insert_idlebusy = """INSERT INTO vpn_log.idle_busy_per_day 
            (
                user_id,
                ldap_user_name,
                vpn_user_id,
                busy_in_hour,
                idle_in_hour,
                date,
                day_type) 
            VALUES 
            (%s, %s,%s, %s, %s,%s, %s)"""
            val = (web_user_id, ldap_user_name, vpn_user_id,
                   session_time_in_hour, idle_time_in_hour, login_date, day_type)
            cursor.execute(insert_idlebusy, val)
            vpn_log.commit()
            print('Added user idle busy info!')
            cursor.close()
            vpn_log.close()
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('User idle busy not inserted.Error happened: ' + str(e))

    @classmethod
    def update_record_in_report_table(cls, *data):
        web_user_id, ldap_user_name, vpn_user_id, new_busy, new_idle, date, day_type = data
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            cursor.execute("""
            UPDATE vpn_log.idle_busy_per_day
            SET user_id=%s,busy_in_hour=%s,idle_in_hour=%s
            WHERE ldap_user_name=%s and date=%s
                """, (web_user_id, new_busy, new_idle, ldap_user_name, date))
            vpn_log.commit()
            print('Updated user idle busy info in users!')
            cursor.close()
            vpn_log.close()
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Update failed during idle busy info. Error happened: ' + str(e))

    @classmethod
    def sync_access_uuid(cls, uuid):
        vpn_log = connect_database()
        if not vpn_log:
            return "Database not connected"
        cursor = vpn_log.cursor()
        try:
            sync_acccess = """INSERT INTO vpn_log.synced_access_hist
            (access_hist_uuid) 
            VALUES 
            (%s)"""
            val = (str(uuid),)
            cursor.execute(sync_acccess, val)
            vpn_log.commit()
            print('Synced access uuid info!')
            cursor.close()
            vpn_log.close()
        except Exception as e:
            cursor.close()
            vpn_log.close()
            print('Sync uuid failed .Error happened: ' + str(e))

    def idle_busy_report_generate(self):
        # find unsynced access uuids
        total_hour_if_in_time_sheet = None
        total_hour_if_not_in_time_sheet = None
        # session_time_in_hour = 0
        # new_idle = 0
        # day_type = 0
        # idle_time_in_hour = 0
        unsunced_access_uuids = self.unsynced_access_uuids()
        for row in unsunced_access_uuids:
            access_uuid = row[0]

        # data : web_id, vpn_user_name, vpn_user_id, busy time, idle time, date, day_type
            access_info_dict = self.access_hist_data_dict(access_uuid)
            if not access_info_dict:
                continue

            check_record_exists_in_report = self.check_if_in_time_sheet(
                access_info_dict['ldap_user_name'])
            start_time = '09:30:00'
            end_time = '18:00:00'
            weekends_default = ['friday', 'saturday']
            start_time_obj = datetime.strptime(start_time, '%H:%M:%S').time()
            end_time_obj = datetime.strptime(end_time, '%H:%M:%S').time()
            login_date = access_info_dict['login_time'].date()
            logout_date = access_info_dict['logout_time'].date()
            login_time = access_info_dict['login_time'].time()
            logout_time = access_info_dict['logout_time'].time()
            day_name_by_login = str(
                access_info_dict['login_time'].strftime("%A")).lower()
            if check_record_exists_in_report:
                date_start = check_record_exists_in_report[0].date()
                date_end = check_record_exists_in_report[1].date()
                time_start = check_record_exists_in_report[0].time()
                time_end = check_record_exists_in_report[1].time()
                time_sheet_id = check_record_exists_in_report[2]
                weekend_array = self.find_week_end(time_sheet_id)
                time_sheet_start_time = datetime.strptime(
                    time_start.strftime("%H:%M:%S"), '%H:%M:%S')
                time_sheet_end_time = datetime.strptime(
                    time_end.strftime("%H:%M:%S"), '%H:%M:%S')
                if date_start <= login_date and date_end >= logout_date:
                    if time_start <= login_time and time_end >= logout_time:
                        session_time = access_info_dict['logout_time'] - \
                            access_info_dict['login_time']
                        total_time = time_sheet_end_time - \
                            time_sheet_start_time
                        total_time_in_hour = total_time.total_seconds()/60/60
                        total_time_in_hour = round(total_time_in_hour, 2)
                        total_hour_if_in_time_sheet = total_time_in_hour
                        # busy time
                        session_time_in_hour = session_time.total_seconds()/60/60
                        session_time_in_hour = round(session_time_in_hour, 2)
                        # idle time
                        idle_time_in_hour = total_time_in_hour - session_time_in_hour

                if day_name_by_login in weekend_array:
                    day_type = 'WE'
                else:
                    day_type = 'WD'

            else:
                if start_time_obj <= login_time and end_time_obj >= logout_time:
                    session_time = access_info_dict['logout_time'] - \
                        access_info_dict['login_time']

                    total_time = datetime.strptime(
                        end_time, '%H:%M:%S') - datetime.strptime(start_time, '%H:%M:%S')
                    total_time_in_hour = total_time.total_seconds()/60/60
                    total_time_in_hour = round(total_time_in_hour, 2)
                    total_hour_if_not_in_time_sheet = total_time_in_hour
                    # busy time
                    session_time_in_hour = session_time.total_seconds()/60/60
                    session_time_in_hour = round(session_time_in_hour, 2)
                    # idle time
                    idle_time_in_hour = total_time_in_hour - session_time_in_hour
                    if day_name_by_login in weekends_default:
                        day_type = 'WE'
                    else:
                        day_type = 'WD'
            check_if_record_exists = self.check_record_exists_in_report(
                access_info_dict['ldap_user_name'], login_date)
            if not check_if_record_exists:
                # print(access_info_dict['web_user_id'], access_info_dict['ldap_user_name'],
                #       access_info_dict['vpn_user_id'], session_time_in_hour, idle_time_in_hour, login_date, day_type)
                self.insert_in_report_table(
                    access_info_dict['web_user_id'], access_info_dict['ldap_user_name'], access_info_dict['vpn_user_id'], session_time_in_hour, idle_time_in_hour, login_date, day_type)
            else:
                old_busy = check_if_record_exists[0]
                old_idle = check_if_record_exists[1]
                new_busy = old_busy + session_time_in_hour
                if total_hour_if_in_time_sheet:
                    new_idle = total_hour_if_in_time_sheet - new_busy
                if total_hour_if_not_in_time_sheet:
                    new_idle = total_hour_if_not_in_time_sheet - new_busy
                # print(access_info_dict['web_user_id'], access_info_dict['ldap_user_name'],
                #       access_info_dict['vpn_user_id'], new_busy, new_idle, login_date, day_type)
                self.update_record_in_report_table(access_info_dict['web_user_id'], access_info_dict['ldap_user_name'],
                                                   access_info_dict['vpn_user_id'], new_busy, new_idle, login_date, day_type)
            self.sync_access_uuid(access_uuid)

        # get access hist date from low to high and date is not today
        # check if that date has entry in timesheet where logout time is not null
        # if not than default time 9.30 to 6.00
        # if there than use that time period
        # check if that username and date has entry in report table
        # find day type
        # if not then insert, busy time same as the login-logout time frame, idle = time frame - busy time
        # if then update busy time = previous + new time frame , idle = time frame - new busy time
