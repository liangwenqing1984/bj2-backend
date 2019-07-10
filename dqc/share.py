import json
import logging
import traceback
import const
import tornado
import tornado.web
import tornado.options
import config
import share_service

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


class ShareHandler(tornado.web.RequestHandler):

    def get(self):
        result = dict()
        code = 0
        msg = const.SUCCESS
        data = []
        try:
            data = share_service.share()
        except Exception:
            warning_message = traceback.format_exc()
            logging.error(warning_message)
            code = 99
            msg = const.UNKNOWN_EXCEPTION
        result['status'] = code
        result['message'] = msg
        result['data'] = data
        self.write(json.dumps(result, ensure_ascii=False))
        self.flush()

    def post(self):
        self.get()


class AccessHandler(tornado.web.RequestHandler):

    def get(self):
        result = dict()
        code = 0
        msg = const.SUCCESS
        data = []
        try:
            data = share_service.access()
        except Exception:
            warning_message = traceback.format_exc()
            logging.error(warning_message)
            code = 99
            msg = const.UNKNOWN_EXCEPTION
        result['status'] = code
        result['message'] = msg
        result['data'] = data
        self.write(json.dumps(result, ensure_ascii=False))
        self.flush()

    def post(self):
        self.get()


def startup():
    tornado.options.define("port", default=config.tornado_port, help="run on the given port", type=int)
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/query/table/list", ShareHandler),
        (r"/access/table/list", AccessHandler),
    ], debug=False)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(tornado.options.options.port)
    logging.getLogger().setLevel(logging.DEBUG)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    startup()
