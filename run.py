import sys
import socketio
import os
import json
import traceback
import shortuuid
import time
import zmq
from app.fxcm import FXCM

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

'''
Utilities
'''
class UserContainer(object):

	def __init__(self):
		self.parent = None
		self.users = {}
		self.add_user_queue = []
		self._setup_zmq_connections()


	def _setup_zmq_connections(self):
		self.zmq_context = zmq.Context()

		self.zmq_req_socket = self.zmq_context.socket(zmq.DEALER)
		self.zmq_req_socket.connect("tcp://zmq_broker:5557")

		self.zmq_pull_socket = self.zmq_context.socket(zmq.PULL)
		self.zmq_pull_socket.connect("tcp://zmq_broker:5558")

		self.zmq_poller = zmq.Poller()
		self.zmq_poller.register(self.zmq_pull_socket, zmq.POLLIN)
		self.zmq_poller.register(self.zmq_req_socket, zmq.POLLIN)


	def setParent(self, parent):
		self.parent = parent


	def getParent(self):
		return self.parent


	def addUser(self, username, password, is_demo, is_parent):
		if username not in self.users:
			self.users[username] = FXCM(self, username, password, is_demo, is_parent=is_parent)
			if is_parent:
				self.parent = self.users[username]

		return self.users[username]


	def deleteUser(self, username):
		if username in self.users:
			self.users[username].stop()
			del self.users[username]


	def getUser(self, username):
		return self.users.get(username)

	
	def addToUserQueue(self):
		_id = shortuuid.uuid()
		self.add_user_queue.append(_id)
		while self.add_user_queue[0] != _id:
			time.sleep(0.1)


	def popUserQueue(self):
		del self.add_user_queue[0]


def getConfig():
	path = os.path.join(ROOT_DIR, 'instance/config.json')
	if os.path.exists(path):
		with open(path, 'r') as f:
			return json.load(f)
	else:
		raise Exception('Config file does not exist.')


'''
Initialize
'''

config = getConfig()
# sio = socketio.Client()
user_container = UserContainer()

'''
Socket IO functions
'''

def sendResponse(msg_id, res):
	res = {
		"type": "broker_reply",
		"message": {
			'msg_id': msg_id,
			'result': res
		}
	}

	user_container.zmq_req_socket.send_json(res)

# def sendResponse(msg_id, res):
# 	res = {
# 		'msg_id': msg_id,
# 		'result': res
# 	}

# 	sio.emit(
# 		'broker_res', 
# 		res, 
# 		namespace='/broker'
# 	)


def onAddUser(username, password, is_demo, is_parent=False):
	user_container.addToUserQueue()
	try:
		user = user_container.addUser(username, password, is_demo, is_parent=is_parent)
	except Exception:
		print(traceback.format_exc())
	finally:
		user_container.popUserQueue()
	
	return {
		'completed': True
	}


def onDeleteUser(username):
	user_container.deleteUser(username)

	return {
		'completed': True
	}


def getUser(username):
	return user_container.getUser(username)


def getParent():
	return user_container.getParent()


# Download Historical Data EPT
def _download_historical_data_broker( 
	user, product, period, tz='Europe/London', 
	start=None, end=None, count=None,
	include_current=True,
	**kwargs
):
	return user._download_historical_data_broker(
		product, period, tz='Europe/London', 
		start=start, end=end, count=count,
		**kwargs
	)


def _subscribe_chart_updates(user, msg_id, instrument):
	user._subscribe_chart_updates(msg_id, instrument)
	return {
		'completed': True
	}


# Create Position EPT

# Modify Position EPT

# Delete Position EPT

# Create Order EPT

# Modify Order EPT

# Delete Order EPT

# Get Account Details EPT

# Get All Accounts EPT


# @sio.on('connect', namespace='/broker')
# def onConnect():
# 	print('CONNECTED!', flush=True)


# @sio.on('disconnect', namespace='/broker')
# def onDisconnect():
# 	print('DISCONNECTED', flush=True)


# @sio.on('broker_cmd', namespace='/broker')
def onCommand(data):
	print(f'COMMAND: {data}', flush=True)

	try:
		cmd = data.get('cmd')
		broker = data.get('broker')

		if broker == 'fxcm':
			res = {}
			if cmd == 'add_user':
				res = onAddUser(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'delete_user':
				res = onDeleteUser(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_download_historical_data_broker':
				user = getParent()
				res = _download_historical_data_broker(user, *data.get('args'), **data.get('kwargs'))

			elif cmd == '_subscribe_chart_updates':
				user = getParent()
				res = _subscribe_chart_updates(user, *data.get('args'), **data.get('kwargs'))

			elif cmd == 'heartbeat':
				user = getParent()
				if user is not None:
					res = { "result": True }
				else:
					res = { "result": False }

			sendResponse(data.get('msg_id'), res)

	except Exception as e:
		print(traceback.format_exc(), flush=True)
		sendResponse(data.get('msg_id'), {
			'error': str(e)
		})

# def createApp():
# 	print('CREATING APP')
# 	while True:
# 		try:
# 			sio.connect(
# 				config['STREAM_URL'], 
# 				headers={
# 					'Broker': 'fxcm'
# 				}, 
# 				namespaces=['/broker']
# 			)
# 			break
# 		except Exception:
# 			pass

# 	# PARENT_USER_CONFIG = config['PARENT_USER']
# 	# parent = FXCM(**PARENT_USER_CONFIG)
# 	# user_container.setParent(parent)

# 	return sio


def run():
	while True:
		socks = dict(user_container.zmq_poller.poll())

		if user_container.zmq_pull_socket in socks:
			message = user_container.zmq_pull_socket.recv_json()
			print(f"[ZMQ_PULL] {message}")
			onCommand(message)

		if user_container.zmq_req_socket in socks:
			message = user_container.zmq_req_socket.recv()
			print(f"[ZMQ_REQ] {message}")


if __name__ == '__main__':
	# sio = createApp()
	run()
