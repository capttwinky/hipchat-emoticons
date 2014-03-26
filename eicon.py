# -*- coding: utf-8 -*-

import jinja2
import hypchat

class HipChatter(object):
    def __init__(self, tkn_path='.'):
        with open('{0}/hc_token.secret'.format(tkn_path)) as ofile:
            self.hc_token = ofile.read()
        self.hc_if = hypchat.HypChat(self.hc_token)
        self.emoticons = []
    
    def load_emoticons(self):
        while True:
            eicons = self.hc_if.emoticons()
            self.emoticons.extend(eicons['items'])
            if eicons['links'].get('next'):
                self.hc_if.emoticons.url = eicons['links'].get('next')
            else:
                break

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
    render_tpl([dict(e) for e in my_hc.emoticons])

if __name__ == '__main__':
    main()
