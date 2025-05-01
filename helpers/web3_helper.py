import logging

from app.core.config import distribution_contract

logger = logging.getLogger(__name__)

def get_events_in_batches(start_block, end_block, event_name, batch_size):
    """Process blockchain events in batches to handle large block ranges"""
    current_start = start_block
    while current_start <= end_block:
        current_end = min(current_start + batch_size, end_block)
        try:
            yield from get_events(current_start, current_end, event_name)
        except Exception as e:
            logger.error(f"Error getting events from block {current_start} to {current_end}: {str(e)}")
        current_start = current_end + 1

def get_events(from_block, to_block, event_name):
    """Get blockchain events for the specified block range"""
    try:
        event_filter = getattr(distribution_contract.events, event_name).create_filter(from_block=from_block, to_block=to_block)
        return event_filter.get_all_entries()
    except Exception as e:
        logger.error(f"Error getting events for {event_name} from block {from_block} to {to_block}: {str(e)}")
        return []

def get_event_headers(event_name):
    event_abi = next((e for e in distribution_contract.abi if e['type'] == 'event' and e['name'] == event_name), None)
    if not event_abi:
        raise ValueError(f"Event {event_name} not found in ABI")
    return ['timestamp', 'transaction_hash', 'block_number'] + [input['name'].lower() for input in event_abi['inputs']]

