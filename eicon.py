# -*- coding: utf-8 -*-

import jinja2
import hypchat
import time
import json

class HipChatter(object):
    http_pause = 300
    
    def __init__(self, tkn_path='.'):
        with open('{0}/hc_token.secret'.format(tkn_path)) as ofile:
            self.hc_token = ofile.read()
        self.hc_if = hypchat.HypChat(self.hc_token)
        self.emoticons = []
        self.exp_icons = []

    def load_emoticons(self):
        self.download_emoticons()
        self.expand_emoticons()
        self.sanatize_records()

    def download_emoticons(self):
        while True:
            eicons = self.hc_if.emoticons()
            if not eicons['items']:
                break
            self.emoticons.extend(eicons['items'])
            self.hc_if.emoticons.url = eicons['links'].get('next')
        self.emoticons.sort(key=lambda x:x['shortcut'])

    def expand_emoticons(self):
        self._expandable_icons = list(self.emoticons)
        print "fetching expanded eicon records"
        while self._expandable_icons:
            try:
                hcico = self._expandable_icons.pop()
                self.exp_icons.append(self.hc_if.get_emoticon(hcico['id']))
            except Exception as E:
                print "pausing for {} seconds".format(self.http_pause)
                self._expandable_icons.append(hcico)
                time.sleep(self.http_pause)
            finally:
                print '\t{}'.format(hcico['shortcut'])
        self.exp_icons.sort(key=lambda x:x['shortcut'])

    def sanatize_records(self):
        for eico in self.exp_icons:
            if eico['creator'] is None:
                eico['creator'] = {'mention_name':None, 'name':'unknown'}


def download_files(recs):
    '''downloads the files assoicated with a list of emoticon records'''
    for micon in recs:
        mpic = requests.get(micon['url'], stream = True)
        with open('{0}.{1}'.format(micon['shortcut'], micon['url'].rpartition('.')[2]), 'wb') as ofile:
            for chunk in mpic.iter_content():
                ofile.write(chunk)
        print "{0} ok".format(micon['shortcut'])

def render_tpl(recs):
    '''render list of emoticon records into html'''
    tenv = jinja2.Environment(
        loader = jinja2.FileSystemLoader('./template'))
    mtpl = tenv.get_template('eicon_template.html')
    with open('emoticons_output.html', 'w') as ofile:
        ofile.write(mtpl.render({'eicons':recs}))

def main():
    my_hc = HipChatter()
    my_hc.load_emoticons()
    with open('eicon_records.out','w') as ofile:
        ofile.write(json.dumps(my_hc.exp_icons))
    render_tpl([dict(e) for e in my_hc.exp_icons])
    print "done"

if __name__ == '__main__':
    main()
