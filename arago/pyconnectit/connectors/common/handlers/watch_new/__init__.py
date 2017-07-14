import gevent
import time, traceback
from greenlet import GreenletExit
from arago.pyconnectit.connectors.common.handlers.base_handler import BaseHandler
from arago.pyconnectit.common.delta_store import KeyNotFoundError, DeltaStoreFull
from arago.pyconnectit.common.lmdb_queue import Empty, Full
from arago.pyconnectit.connectors.netcool.handlers.sync_netcool_status import StatusUpdate

class Watch(BaseHandler):
	def __init__(self, watchlist_map, watchers, queue_map):
		super().__init__()
		self.watchlist_map=watchlist_map
		self.watchers=watchers
		self.queue_map=queue_map
		for env in watchlist_map.keys():
			self.logger.info("Loading list of previously watched events from {ds}".format(ds=watchlist_map[env]))
			for event_id, age in watchlist_map[env].get_event_ages():
				self.logger.debug("Watched event {ev} is {age} seconds old".format(ev=event_id, age=age//1000))
				remaining_time = 30000 - age
				data=watchlist_map[env].get_merged(event_id)
				if remaining_time > 0:
					self.logger.verbose("Setting timeout for event {ev} to {secs} seconds".format(
						secs=remaining_time//1000, ev=event_id))
					self.watchers[data['mand']['eventId']]=gevent.spawn(self.watch, data, env, remaining_time/1000)
				else:
					self.logger.verbose(
						"Event {ev} is already stale, setting status to 'No issue created'.".format(ev=event_id))
					self.watchers[data['mand']['eventId']]=gevent.spawn(self.watch, data, env, 0)

	def watch(self, event, env, timeout):
		try:
			event_id = event['mand']['eventId']
			gevent.sleep(timeout)
			try:
				self.logger.verbose(
					"Event {ev} found to be stale, setting status to 'No issue created'.".format(ev=event_id))
				try:
					event['free']['eventNormalizedStatus'].append({'value':'No_issue_created',
																   'timestamp':str(int(time.time() * 1000))})
				except KeyError as e:
					if e.args[0] == 'eventNormalizedStatus':
						event['free']['eventNormalizedStatus'] = [
							{'value': 'No_issue_created',
							 'timestamp':str(int(time.time() * 1000))}]
					elif e.args[0] == 'free':
						event['free'] = {'eventNormalizedStatus': [
								{'value': 'No_issue_created',
								 'timestamp':str(int(time.time() * 1000))}]}
					else:
						raise
				status_update = StatusUpdate(event_id, event)
				self.queue_map[env].put(status_update)
				self.watchlist_map[env].delete(event_id)
			except Full:
				raise QueuingError("Queue full")
			except DeltaStoreFull as e:
				self.logger.critical("Watchlist for {env} can't delete this event: {err}".format(
					env=self.env, err=e))
			except KeyNotFoundError as e:
				self.logger.warn(e)
			except KeyError as e:
				self.logger.warn("No queue defined for {env}".format(
					env=self.env))
		except GreenletExit:
			self.watchlist_map[env].delete(event_id)
			self.logger.info("Watcher for event {ev} canceled".format(ev=event_id))

	def __call__(self, data, env):
		try:
			self.watchlist_map[env].append(data['mand']['eventId'], data)
			self.watchers[data['mand']['eventId']]=gevent.spawn(self.watch, data, env, 30)
			self.logger.verbose("Starting to watch new Event {ev}".format(
				ev=data['mand']['eventId']))
		except DeltaStoreFull as e:
			self.logger.critical("Watchlist for {env} can't store this event: {err}".format(env=env, err=e))
		except KeyError:
			self.logger.warning(
				"No Watchlist defined for environment: {env}".format(
					env=env))

	def __str__(self):
		return "Watch"


class Unwatch(BaseHandler):
	def __init__(self, watchlist_map, watchers):
		super().__init__()
		self.watchlist_map=watchlist_map
		self.watchers=watchers

	def __call__(self, data, env):
		try:
			if self.watchers[data['mand']['eventId']]:
				self.watchers[data['mand']['eventId']].kill()
			else:
				self.logger.info("Already dead!")
			# self.watchlist_map[env].delete(data['mand']['eventId'])
			self.logger.verbose("Stopping to watch Event {ev}".format(
				ev=data['mand']['eventId']))
		except DeltaStoreFull as e:
			self.logger.critical("Watchlist for {env} can't delete this event: {err}".format(env=env, err=e))
		except KeyNotFoundError as e:
			self.logger.debug(e)
		except KeyError as e:
			if e.args[0] == env:
				self.logger.warning(
					"No Watchlist defined for environment: {env}".format(
						env=env))
			else:
				self.logger.error(
					"Removing event {ev} failed with an unknown error:\n".format(ev=data['mand']['eventId'])
					+ traceback.format_exc())

	def __str__(self):
		return "Unwatch"
