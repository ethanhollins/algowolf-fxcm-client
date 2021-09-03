import numpy as np
import pandas as pd
import time
import ntplib
import shortuuid
import traceback
from . import tradelib as tl
from threading import Thread
from forexconnect import ForexConnect, fxcorepy, Common
from datetime import datetime
from threading import Thread


class OffersTableListener(object):
	def __init__(self, instruments=[], listeners=[]):
		self.__instruments = instruments
		self.__listeners = listeners
		self._running = False

	def addInstrument(self, instrument, listener):
		if instrument not in self.__instruments:
			self.__instruments.append(instrument)
			self.__listeners.append(listener)

	def on_added(self, table_listener, row_id, row):
		pass

	def on_changed(self, table_listener, row_id, row):
		if row.table_type == ForexConnect.OFFERS:
			self.print_offer(row, self.__instruments, self.__listeners)

	def on_deleted(self, table_listener, row_id, row):
		pass

	def on_status_changed(self, table_listener, status):
		pass

	def print_offer(self, offer_row, selected_instruments, listeners):
		if self._running:
			offer_id = offer_row.offer_id
			instrument = offer_row.instrument
			time = offer_row.time
			bid = round(offer_row.bid, 5)
			ask = round(offer_row.ask, 5)
			volume = offer_row.volume

			try:
				idx = selected_instruments.index(instrument)
				listener = listeners[idx]
				listener(time, bid, ask, volume)

			except ValueError:
				pass

	def start(self):
		self._running = True

	def stop(self):
		self._running = False


class Subscription(object):

	def __init__(self, broker, instrument):
		self.broker = broker
		self.msg_ids = []
		self.instrument = instrument


	def onChartUpdate(self, *args, **kwargs):
		args = list(args)
		args[0] = tl.utils.convertTimeToTimestamp(args[0])

		for msg_id in self.msg_ids:
			self.broker.send_queue.append((
				msg_id,
				{
					'args': args,
					'kwargs': kwargs
				}
			))


class FXCM(object):

	def __init__(self, sio, username, password, is_demo, is_parent):
		self.sio = sio
		self.username = username
		self.password = password
		self.is_demo = is_demo

		self.is_parent = is_parent
		self.job_queue = []
		self.send_queue = []
		self.subscriptions = {}
		self.offers_listener = None

		self.fx = ForexConnect()
		self.session = None

		self._login()

		if is_parent:
			while self.session is None or self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTING:
				time.sleep(0.01)

			# if self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED:
			# 	self._get_offers_listener()

		Thread(target=self._send_response).start()


	def _is_logged_in(self):
		if not self.session is None:
			if (
				self.session.session_status != fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED and
				tl.utils.isWeekend(datetime.utcnow())
			):
				return True

			while self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTING:
				time.sleep(0.01)

			# print(F'[FXCM] Is logged in: {self.session.session_status}')
			return self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED
		return False


	def _login(self):
		if not self._is_logged_in():
			try:
				print('[FXCM] Attempting login...', flush=True)
				self.fx.login(
					user_id=self.username, password=self.password, 
					connection='demo' if self.is_demo else 'real',
					session_status_callback=self._on_status_change
				)
				return True

			except Exception:
				# print(traceback.format_exc(), flush=True)
				print('[FXCM] Login failed.', flush=True)
				return False

		else:
			return True


	def stop(self):
		self.fx.logout()


	def _set_time_off(self):
		try:
			client = ntplib.NTPClient()
			response = client.request('pool.ntp.org')
			self.time_off = response.tx_time - time.time()
		except Exception:
			pass


	def _get_offers_listener(self):
		if self.offers_listener is not None:
			self.offers_listener.stop()

		offers = self.fx.get_table(ForexConnect.OFFERS)
		self.offers_listener = OffersTableListener()

		Common.subscribe_table_updates(
			offers,
			on_change_callback=self.offers_listener.on_changed,
			on_add_callback=self.offers_listener.on_added,
			on_delete_callback=self.offers_listener.on_deleted,
			on_status_change_callback=self.offers_listener.on_changed
		)
		self._resubscribe_chart_updates()

		self.offers_listener.start()


	def _handle_job(self, func, *args, **kwargs):
		ref_id = shortuuid.uuid()
		self.job_queue.append(ref_id)
		while self.job_queue.index(ref_id) > 0: pass
		result = func(*args, **kwargs)
		del self.job_queue[0]
		return result


	def _on_status_change(self, session, status):
		self.session = session

		print(f"Trading session status: {status}", flush=True)
		if status in (
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.DISCONNECTED,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.SESSION_LOST,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.RECONNECTING,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.PRICE_SESSION_RECONNECTING,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.CHART_SESSION_RECONNECTING
		):
			print('[FXCM] Disconnected.', flush=True)
			if not tl.utils.isWeekend(datetime.utcnow()):
				try:
					self.session.logout()
				except Exception:
					pass
				finally:
					self.session = None

				time.sleep(1)
				self._login()

			# sys.exit()

		elif status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED:
			print('[FXCM] Logged in.', flush=True)
			Thread(target=self._get_offers_listener).start()
			# if self.offers_listener is not None:
			# 	offers = self.fx.get_table(ForexConnect.OFFERS)
			# 	table_listener = Common.subscribe_table_updates(
			# 		offers,
			# 		on_change_callback=self.offers_listener.on_changed,
			# 		on_add_callback=self.offers_listener.on_added,
			# 		on_delete_callback=self.offers_listener.on_deleted,
			# 		on_status_change_callback=self.offers_listener.on_changed
			# 	)
				
			# if self._initialized and self.offers_listener is None:
			# 	self.data_saver.fill_all_missing_data()


	# def _get_offers_listener(self):
	# 	offers = self.fx.get_table(ForexConnect.OFFERS)
	# 	self.offers_listener = OffersTableListener()

	# 	table_listener = Common.subscribe_table_updates(
	# 		offers,
	# 		on_change_callback=self.offers_listener.on_changed,
	# 		on_add_callback=self.offers_listener.on_added,
	# 		on_delete_callback=self.offers_listener.on_deleted,
	# 		on_status_change_callback=self.offers_listener.on_changed
	# 	)


	def _send_response(self):
		while True:
			if len(self.send_queue) > 0:
				msg_id, res = self.send_queue[0]
				
				try:
					res = {
						'msg_id': msg_id,
						'result': res
					}
					self.sio.emit(
						'broker_res', 
						res, 
						namespace='/broker'
					)

				except Exception:
					print(f"[FXCM._send_response] {time.time()}: {traceback.format_exc()}")
				
				del self.send_queue[0]

			time.sleep(0.01)

	'''
	Broker functions
	'''

	def _download_historical_data_broker(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		**kwargs
	):
		# Count
		if not count is None:
			res = self._handle_job(
				self.fx.get_history,
				self._convert_product(product), 
				self._convert_period(period), 
				quotes_count=count
			)

		# Start -> End
		else:
			start = tl.utils.convertTimestampToTime(start)
			start = start.replace(tzinfo=None)
			end = tl.utils.convertTimestampToTime(end)
			end = end.replace(tzinfo=None)
			res = self._handle_job(
				self.fx.get_history,
				self._convert_product(product), 
				self._convert_period(period), 
				start, end
			)

		# Convert to result DF
		res = np.array(list(map(lambda x: list(x), res)))

		ask_prices = res[:, 5:9].astype(float)
		bid_prices = res[:, 1:5].astype(float)
		mid_prices = (ask_prices + bid_prices)/2
		timestamps = res[:, 0]
		prices = np.around(np.concatenate((ask_prices, mid_prices, bid_prices), axis=1), decimals=5)

		result = pd.DataFrame(
			index=pd.Index(timestamps).map(
				lambda x: int((x - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))
			).rename('timestamp'),
			columns=[
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'mid_open', 'mid_high', 'mid_low', 'mid_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			],
			data=prices
		)

		return result.to_dict()


	def _resubscribe_chart_updates(self):
		start_time = time.time()
		while self.offers_listener is None or not self.offers_listener._running:
			if time.time() - start_time > 30:
				return
			time.sleep(1)

		for i in self.subscriptions:
			sub = self.subscriptions[i]
			self.offers_listener.addInstrument(
				self._convert_product(sub.instrument), 
				sub.onChartUpdate
			)


	def _subscribe_chart_updates(self, msg_id, instrument):
		print(f"[FXCM._subscribe_chart_updates] SUBSCRIBE: {msg_id}, {instrument}", flush=True)
		start_time = time.time()
		while self.offers_listener is None or not self.offers_listener._running:
			if time.time() - start_time > 30:
				return
			time.sleep(1)

		if instrument in self.subscriptions:
			print(f"[FXCM._subscribe_chart_updates] SECOND:", flush=True)
			subscription = self.subscriptions[instrument]
			subscription.msg_ids.append(msg_id)
			
		else:
			print(f"[FXCM._subscribe_chart_updates] FIRST:", flush=True)
			subscription = Subscription(self, instrument)
			subscription.msg_ids.append(msg_id)
			self.subscriptions[instrument] = subscription

			self.offers_listener.addInstrument(
				self._convert_product(instrument), 
				subscription.onChartUpdate
			)
		

	def _convert_product(self, product):
		return product.replace('_', '/')


	def _convert_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return 'm1'
		elif period == tl.period.TWO_MINUTES:
			return 'm2'
		elif period == tl.period.THREE_MINUTES:
			return 'm3'
		elif period == tl.period.FIVE_MINUTES:
			return 'm5'
		elif period == tl.period.TEN_MINUTES:
			return 'm10'
		elif period == tl.period.FIFTEEN_MINUTES:
			return 'm15'
		elif period == tl.period.THIRTY_MINUTES:
			return 'm30'
		elif period == tl.period.ONE_HOUR:
			return 'H1'
		elif period == tl.period.TWO_HOURS:
			return 'H2'
		elif period == tl.period.THREE_HOURS:
			return 'H3'
		elif period == tl.period.FOUR_HOURS:
			return 'H4'
		elif period == tl.period.DAILY:
			return 'D1'
		elif period == tl.period.WEEKLY:
			return 'W1'
		elif period == tl.period.MONTHLY:
			return 'M1'