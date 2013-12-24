#!/usr/bin/python
import ConfigParser
import urllib2
import sqlite3
import time

def configReader(fileName):
    config = ConfigParser.ConfigParser()
    config.read(fileName)
    return config

def getDbConn(fileName):
    conn = sqlite3.connect(fileName)
    return conn

def importPlaylist(listReader):
    return

def main():
    config = configReader('config.cfg')
    link = config.get('Main', 'playlist')
    dbFile = config.get('db', 'name')
    dbConn = getDbConn(dbFile)
    parentId = config.get('db', 'categoryId')
    clearCategory(dbConn, parentId);
    transferPlaylist(link, parentId, dbConn)
    dbConn.close()

def makeConfig(fileName):
    config = ConfigParser.RawConfigParser()
    config.add_section('Main')
    config.set('Main', 'playlist', 'http://api.torrent-tv.ru/t/gAOC2V4LrlKnQl94ncPseg2ImP9I2VeUs%2FwxdmgkRvsSVxhXWv5UwlIWv6XuPdMo')
    config.add_section('db')
    config.set('db', 'name', '/Users/vitek/python/torrentTvToMediatomb/mediatomb.db')
    config.set('db', 'folderName', 'Torrent-TV')
    config.set('db', 'categoryId', '17')

    with open('config.cfg', 'wb') as configfile:
        config.write(configfile)

def transferPlaylist(link, categoryId, dbConn):
    u = urllib2.urlopen(link)
    columns = ('ref_id',\
               'parent_id',\
               'object_type',\
               'upnp_class',\
               'dc_title',\
               'location',\
               'location_hash',\
               'metadata',\
               'auxdata',\
               'resources',\
               'update_id',\
               'mime_type',\
               'flags',\
               'track_number',\
               'service_id'
    )
    columnsSql = "'" + "','".join(columns) + "'"
    cursor = dbConn.cursor()
    parentCategoryLocationCursor = cursor.execute('SELECT location FROM mt_cds_object WHERE id = ' + str(categoryId))
    parentCategoryLocation = parentCategoryLocationCursor.fetchone()
    parentCategoryLocation = parentCategoryLocation[0]

    categories = dict()
    fakeId = 0
    for row in u:
        if row.startswith('#EXTINF'):
            channelName = row.split(',')[1].rstrip('\n')
            lastScopePos = channelName.rfind('(')
            if lastScopePos > 0:
                categoryName = channelName[lastScopePos+1:-1]
                channelName = channelName[:lastScopePos-1]
            else:
                categoryName = 'HZ'
            if not categoryName in categories:
                categoryLocation = parentCategoryLocation.encode('ascii') + '/' + categoryName

                dbRow = ('',\
                         str(categoryId),\
                         '1',\
                         'object.container',\
                         categoryName,\
                         categoryLocation ,\
                         str(getHash(categoryLocation)),\
                         '',\
                         '',\
                         '',\
                         '0',\
                         '',\
                         '1',\
                         '',\
                         ''
                )
                sql = 'INSERT INTO mt_cds_object(' + columnsSql + \
                             ") VALUES ('" + "','".join(dbRow) + "')"
                cursor.execute(sql)
                categories[categoryName] = cursor.lastrowid

                print 'new category: ' + categoryName + ' id #' + str(categories[categoryName])
        elif row.startswith('#'):
            continue
        else:
            streamHash = 'http://192.168.100.28:8000/pid/' + row.rstrip('\n')
            dbRow = ('',\
                     str(categories[categoryName]),\
                     '10',\
                     'object.item.videoItem',\
                     channelName,\
                     streamHash,\
                     '',\
                     'dc%3Adescription=Torrentstream',\
                     '',\
                     '0~protocolInfo=http-get%3A%2A%3Avideo%2FH264%3A%2A~~',\
                     '0',\
                     'video/H264',\
                     '1',\
                     '',\
                     ''
            )

            sql = 'INSERT INTO mt_cds_object(' + columnsSql + \
                             ") VALUES ('" + "','".join(dbRow) + "')"
            print 'channel ' + channelName + ':' + categoryName
            cursor.execute(sql)
    dbConn.commit()

def log(message):
    print time.strftime('%Y-%m-%d %H:%M:%S: ') + message

def clearCategory(dbConn, id):
    log('clearing category ' + str(id))
    cursor = dbConn.cursor()
    result = cursor.execute('SELECT id from mt_cds_object WHERE parent_id = 17')
    ids = list(int(i[0]) for i in result)
    clearSubcat(dbConn, ids)

def clearSubcat(dbConn, ids):
    cursor = dbConn.cursor()
    sqlIds =  ','.join(str(x) for x in ids)
    result = cursor.execute('SELECT id from mt_cds_object WHERE parent_id in (' + sqlIds + ')')
    ids = list(int(i[0]) for i in result)
    if ids:
        clearSubcat(dbConn, ids)
    sql = 'DELETE FROM mt_cds_object WHERE id in (' + sqlIds + ')'
    cursor.execute(sql)
    dbConn.commit()

def getHash(string):
    hash = 5381
    bits32 = 2 ** 32 - 1

    for c in string:
        hash = ((hash << 5) + hash) ^ ord(c)
    return hash & bits32

if __name__ == '__main__':
    #makeConfig('config.cfg')
    main()
