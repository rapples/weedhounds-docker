"""
DHT Peer Network Manager for WeedHounds
Manages unlimited peer-to-peer connections and data sharing
"""

import asyncio
import aiohttp
import json
import logging
import socket
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import ipaddress

from dht_storage import DHTStorage, CacheEntry, DataType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PeerStatus(Enum):
    """Peer connection status"""
    UNKNOWN = "unknown"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    FAILED = "failed"

@dataclass
class PeerNode:
    """Represents a peer node in the network"""
    node_id: str
    address: str
    port: int
    last_seen: datetime
    status: PeerStatus = PeerStatus.UNKNOWN
    latency_ms: float = 0.0
    cache_entries: int = 0
    storage_capacity_gb: float = 0.0
    storage_used_gb: float = 0.0
    api_capabilities: List[str] = None
    reliability_score: float = 1.0
    data_shared: int = 0
    data_received: int = 0
    
    def __post_init__(self):
        if self.api_capabilities is None:
            self.api_capabilities = []
    
    @property
    def full_address(self) -> str:
        return f"{self.address}:{self.port}"
    
    @property
    def storage_usage_percent(self) -> float:
        if self.storage_capacity_gb == 0:
            return 0.0
        return (self.storage_used_gb / self.storage_capacity_gb) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['last_seen'] = self.last_seen.isoformat()
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PeerNode':
        data['last_seen'] = datetime.fromisoformat(data['last_seen'])
        data['status'] = PeerStatus(data['status'])
        return cls(**data)

class PeerNetworkManager:
    """Manages the peer-to-peer network for unlimited cannabis data sharing"""
    
    def __init__(self, node_id: str, listen_port: int = 9100, 
                 max_peers: int = 1000, storage: Optional[DHTStorage] = None):
        self.node_id = node_id
        self.listen_port = listen_port
        self.max_peers = max_peers
        self.storage = storage
        
        # Peer management
        self.peers: Dict[str, PeerNode] = {}
        self.peer_sessions: Dict[str, aiohttp.ClientSession] = {}
        self.connection_pool_size = 50
        
        # Network discovery
        self.bootstrap_nodes: List[Tuple[str, int]] = []
        self.discovery_interval = 30  # seconds
        self.discovery_methods = ['mdns', 'bootstrap', 'peer_exchange']
        
        # Data sharing
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.request_timeout = 30  # seconds
        self.max_concurrent_requests = 20
        
        # Network statistics
        self.stats = {
            'peers_discovered': 0,
            'peer_connections': 0,
            'peer_disconnections': 0,
            'data_requests_sent': 0,
            'data_requests_received': 0,
            'data_responses_sent': 0,
            'data_responses_received': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'network_errors': 0
        }
        
        # HTTP server for peer communication
        self.app = aiohttp.web.Application()
        self.setup_routes()
        self.server = None
    
    def setup_routes(self):
        """Setup HTTP routes for peer communication"""
        # Peer discovery and handshake
        self.app.router.add_post('/peer/handshake', self.handle_handshake)
        self.app.router.add_get('/peer/info', self.handle_peer_info)
        self.app.router.add_get('/peer/ping', self.handle_ping)
        
        # Data sharing
        self.app.router.add_post('/data/request', self.handle_data_request)
        self.app.router.add_post('/data/offer', self.handle_data_offer)
        self.app.router.add_get('/data/availability', self.handle_data_availability)
        
        # Network management
        self.app.router.add_get('/network/peers', self.handle_peer_list)
        self.app.router.add_post('/network/announce', self.handle_peer_announcement)
        self.app.router.add_get('/network/stats', self.handle_network_stats)
    
    async def start(self, bootstrap_nodes: List[Tuple[str, int]] = None):
        """Start the peer network manager"""
        try:
            if bootstrap_nodes:
                self.bootstrap_nodes.extend(bootstrap_nodes)
            
            # Start HTTP server
            runner = aiohttp.web.AppRunner(self.app)
            await runner.setup()
            site = aiohttp.web.TCPSite(runner, '0.0.0.0', self.listen_port)
            await site.start()
            self.server = runner
            
            # Start background tasks
            asyncio.create_task(self.peer_discovery_loop())
            asyncio.create_task(self.peer_maintenance_loop())
            asyncio.create_task(self.data_sharing_loop())
            
            logger.info(f"Peer network started on port {self.listen_port}")
            logger.info(f"Node ID: {self.node_id}")
            logger.info(f"Max peers: {self.max_peers}")
            
            # Perform initial discovery
            await self.discover_initial_peers()
            
        except Exception as e:
            logger.error(f"Failed to start peer network: {e}")
            raise
    
    async def stop(self):
        """Stop the peer network manager"""
        try:
            # Close all peer sessions
            for session in self.peer_sessions.values():
                await session.close()
            self.peer_sessions.clear()
            
            # Stop HTTP server
            if self.server:
                await self.server.cleanup()
            
            logger.info("Peer network stopped")
            
        except Exception as e:
            logger.error(f"Error stopping peer network: {e}")
    
    async def discover_initial_peers(self):
        """Discover initial peers using various methods"""
        discovered = 0
        
        # Bootstrap nodes
        for address, port in self.bootstrap_nodes:
            if await self.connect_to_peer(address, port):
                discovered += 1
        
        # mDNS discovery (simulated for now)
        mdns_peers = await self.discover_mdns_peers()
        for address, port in mdns_peers:
            if await self.connect_to_peer(address, port):
                discovered += 1
        
        # Local network scan
        local_peers = await self.scan_local_network()
        for address, port in local_peers:
            if await self.connect_to_peer(address, port):
                discovered += 1
        
        logger.info(f"Initial discovery found {discovered} peers")
    
    async def discover_mdns_peers(self) -> List[Tuple[str, int]]:
        """Discover peers using mDNS (simulated)"""
        # In a real implementation, this would use actual mDNS
        # For now, return some common development addresses
        potential_peers = [
            ('127.0.0.1', 9101),
            ('127.0.0.1', 9102),
            ('127.0.0.1', 9103),
        ]
        
        verified_peers = []
        for address, port in potential_peers:
            if await self.test_peer_connection(address, port):
                verified_peers.append((address, port))
        
        return verified_peers
    
    async def scan_local_network(self) -> List[Tuple[str, int]]:
        """Scan local network for peers"""
        try:
            # Get local network range
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            network = ipaddress.IPv4Network(f"{local_ip}/24", strict=False)
            
            # Common DHT ports to check
            common_ports = [9100, 9101, 9102, 9103, 9104, 9105]
            
            discovered = []
            tasks = []
            
            # Limit scan to avoid overwhelming the network
            for ip in list(network.hosts())[:50]:  # Scan first 50 IPs
                for port in common_ports:
                    if str(ip) != local_ip:  # Don't scan ourselves
                        tasks.append(self.test_peer_connection(str(ip), port))
            
            # Test connections in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if result is True:
                    ip_index = i // len(common_ports)
                    port_index = i % len(common_ports)
                    ip = str(list(network.hosts())[ip_index])
                    port = common_ports[port_index]
                    discovered.append((ip, port))
            
            logger.info(f"Local network scan found {len(discovered)} potential peers")
            return discovered
            
        except Exception as e:
            logger.error(f"Local network scan failed: {e}")
            return []
    
    async def test_peer_connection(self, address: str, port: int) -> bool:
        """Test if a peer is reachable"""
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"http://{address}:{port}/peer/ping") as response:
                    return response.status == 200
        except:
            return False
    
    async def connect_to_peer(self, address: str, port: int) -> bool:
        """Connect to a specific peer"""
        try:
            if len(self.peers) >= self.max_peers:
                logger.warning("Max peers reached, cannot connect to more")
                return False
            
            # Test connection first
            if not await self.test_peer_connection(address, port):
                return False
            
            # Perform handshake
            peer_info = await self.perform_handshake(address, port)
            if not peer_info:
                return False
            
            # Create peer node
            peer = PeerNode(
                node_id=peer_info['node_id'],
                address=address,
                port=port,
                last_seen=datetime.now(),
                status=PeerStatus.CONNECTED,
                cache_entries=peer_info.get('cache_entries', 0),
                storage_capacity_gb=peer_info.get('storage_capacity_gb', 0),
                storage_used_gb=peer_info.get('storage_used_gb', 0),
                api_capabilities=peer_info.get('api_capabilities', [])
            )
            
            # Add to peer list
            self.peers[peer.node_id] = peer
            
            # Create session for this peer
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.request_timeout)
            )
            self.peer_sessions[peer.node_id] = session
            
            self.stats['peer_connections'] += 1
            self.stats['peers_discovered'] += 1
            
            logger.info(f"Connected to peer {peer.node_id} at {peer.full_address}")
            
            # Exchange peer lists
            await self.exchange_peer_lists(peer)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to peer {address}:{port}: {e}")
            return False
    
    async def perform_handshake(self, address: str, port: int) -> Optional[Dict[str, Any]]:
        """Perform handshake with a peer"""
        try:
            handshake_data = {
                'node_id': self.node_id,
                'version': '1.0',
                'capabilities': ['cannabis_data', 'dht_storage', 'peer_exchange'],
                'storage_info': self.storage.get_storage_stats() if self.storage else {}
            }
            
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"http://{address}:{port}/peer/handshake"
                async with session.post(url, json=handshake_data) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.warning(f"Handshake failed with {address}:{port}: {response.status}")
                        return None
                        
        except Exception as e:
            logger.debug(f"Handshake error with {address}:{port}: {e}")
            return None
    
    async def exchange_peer_lists(self, peer: PeerNode):
        """Exchange peer lists with a connected peer"""
        try:
            if peer.node_id not in self.peer_sessions:
                return
            
            session = self.peer_sessions[peer.node_id]
            url = f"http://{peer.address}:{peer.port}/network/peers"
            
            async with session.get(url) as response:
                if response.status == 200:
                    peer_data = await response.json()
                    remote_peers = peer_data.get('peers', [])
                    
                    # Connect to new peers
                    for remote_peer in remote_peers[:10]:  # Limit to prevent spam
                        if (remote_peer['node_id'] != self.node_id and 
                            remote_peer['node_id'] not in self.peers and
                            len(self.peers) < self.max_peers):
                            
                            asyncio.create_task(
                                self.connect_to_peer(
                                    remote_peer['address'], 
                                    remote_peer['port']
                                )
                            )
                            
        except Exception as e:
            logger.error(f"Failed to exchange peer lists with {peer.node_id}: {e}")
    
    async def request_data_from_network(self, key: str, data_type: DataType) -> Optional[CacheEntry]:
        """Request data from the peer network"""
        try:
            if not self.peers:
                return None
            
            self.stats['data_requests_sent'] += 1
            
            # Select best peers for this request
            suitable_peers = self.select_peers_for_request(data_type)
            if not suitable_peers:
                return None
            
            # Send requests in parallel
            tasks = []
            for peer in suitable_peers[:5]:  # Limit concurrent requests
                task = self.request_data_from_peer(peer, key, data_type)
                tasks.append(task)
            
            # Wait for first successful response
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            
            # Process results
            for task in done:
                try:
                    result = await task
                    if result:
                        self.stats['data_responses_received'] += 1
                        return result
                except Exception as e:
                    logger.debug(f"Peer request failed: {e}")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to request data from network: {e}")
            return None
    
    def select_peers_for_request(self, data_type: DataType) -> List[PeerNode]:
        """Select the best peers for a data request"""
        # Filter peers by capability and status
        suitable_peers = []
        
        for peer in self.peers.values():
            if (peer.status == PeerStatus.CONNECTED and
                peer.reliability_score > 0.5 and
                self.peer_has_capability(peer, data_type)):
                suitable_peers.append(peer)
        
        # Sort by reliability and latency
        suitable_peers.sort(key=lambda p: (p.reliability_score, -p.latency_ms), reverse=True)
        
        return suitable_peers
    
    def peer_has_capability(self, peer: PeerNode, data_type: DataType) -> bool:
        """Check if peer has capability for data type"""
        capability_map = {
            DataType.DUTCHIE_MENU: 'dutchie_api',
            DataType.JANE_MENU: 'jane_api',
            DataType.TERPENE_PROFILE: 'terpene_analysis',
            DataType.STRAIN_INFO: 'strain_database',
            DataType.DISPENSARY_INFO: 'dispensary_data',
            DataType.PRICE_HISTORY: 'price_tracking',
            DataType.INVENTORY_STATUS: 'inventory_monitoring',
            DataType.PRODUCT_REVIEW: 'review_data'
        }
        
        required_capability = capability_map.get(data_type, 'cannabis_data')
        return required_capability in peer.api_capabilities
    
    async def request_data_from_peer(self, peer: PeerNode, key: str, data_type: DataType) -> Optional[CacheEntry]:
        """Request specific data from a peer"""
        try:
            if peer.node_id not in self.peer_sessions:
                return None
            
            session = self.peer_sessions[peer.node_id]
            url = f"http://{peer.address}:{peer.port}/data/request"
            
            request_data = {
                'key': key,
                'data_type': data_type.value,
                'requester': self.node_id
            }
            
            start_time = time.time()
            async with session.post(url, json=request_data) as response:
                latency = (time.time() - start_time) * 1000
                peer.latency_ms = latency
                
                if response.status == 200:
                    data = await response.json()
                    if data.get('found'):
                        # Update peer stats
                        peer.data_shared += 1
                        peer.reliability_score = min(1.0, peer.reliability_score + 0.1)
                        
                        # Create cache entry
                        entry_data = data['entry']
                        return CacheEntry.from_dict(entry_data)
                    else:
                        return None
                else:
                    # Decrease reliability on failure
                    peer.reliability_score = max(0.1, peer.reliability_score - 0.1)
                    return None
                    
        except Exception as e:
            logger.debug(f"Request to peer {peer.node_id} failed: {e}")
            # Decrease reliability on error
            peer.reliability_score = max(0.1, peer.reliability_score - 0.2)
            return None
    
    # HTTP Route Handlers
    
    async def handle_handshake(self, request):
        """Handle peer handshake request"""
        try:
            data = await request.json()
            
            # Validate handshake
            if 'node_id' not in data or data['node_id'] == self.node_id:
                return aiohttp.web.json_response({'error': 'Invalid handshake'}, status=400)
            
            # Respond with our info
            response_data = {
                'node_id': self.node_id,
                'version': '1.0',
                'capabilities': ['cannabis_data', 'dht_storage', 'peer_exchange'],
                'cache_entries': len(self.storage.memory_cache) if self.storage else 0,
                'storage_capacity_gb': self.storage.max_storage_bytes / (1024**3) if self.storage else 0,
                'storage_used_gb': self.storage.current_storage_bytes / (1024**3) if self.storage else 0,
                'api_capabilities': ['dutchie_api', 'jane_api', 'terpene_analysis']
            }
            
            return aiohttp.web.json_response(response_data)
            
        except Exception as e:
            logger.error(f"Handshake handler error: {e}")
            return aiohttp.web.json_response({'error': 'Handshake failed'}, status=500)
    
    async def handle_data_request(self, request):
        """Handle data request from peer"""
        try:
            data = await request.json()
            key = data.get('key')
            data_type_str = data.get('data_type')
            requester = data.get('requester')
            
            if not all([key, data_type_str, requester]):
                return aiohttp.web.json_response({'error': 'Missing parameters'}, status=400)
            
            data_type = DataType(data_type_str)
            
            # Look up data in our storage
            if self.storage:
                entry = await self.storage.get(key, check_peers=False)
                if entry:
                    self.stats['data_responses_sent'] += 1
                    return aiohttp.web.json_response({
                        'found': True,
                        'entry': entry.to_dict()
                    })
            
            self.stats['data_requests_received'] += 1
            return aiohttp.web.json_response({'found': False})
            
        except Exception as e:
            logger.error(f"Data request handler error: {e}")
            return aiohttp.web.json_response({'error': 'Request failed'}, status=500)
    
    async def handle_peer_list(self, request):
        """Handle peer list request"""
        try:
            peers_data = []
            for peer in self.peers.values():
                if peer.status == PeerStatus.CONNECTED:
                    peers_data.append({
                        'node_id': peer.node_id,
                        'address': peer.address,
                        'port': peer.port,
                        'last_seen': peer.last_seen.isoformat(),
                        'reliability_score': peer.reliability_score
                    })
            
            return aiohttp.web.json_response({'peers': peers_data})
            
        except Exception as e:
            logger.error(f"Peer list handler error: {e}")
            return aiohttp.web.json_response({'error': 'Failed to get peer list'}, status=500)
    
    async def handle_ping(self, request):
        """Handle ping request"""
        return aiohttp.web.json_response({
            'pong': datetime.now().isoformat(),
            'node_id': self.node_id
        })
    
    async def handle_network_stats(self, request):
        """Handle network statistics request"""
        try:
            stats = {
                'node_id': self.node_id,
                'peer_count': len(self.peers),
                'connected_peers': len([p for p in self.peers.values() if p.status == PeerStatus.CONNECTED]),
                'network_stats': self.stats,
                'storage_stats': self.storage.get_storage_stats() if self.storage else {}
            }
            
            return aiohttp.web.json_response(stats)
            
        except Exception as e:
            logger.error(f"Network stats handler error: {e}")
            return aiohttp.web.json_response({'error': 'Failed to get stats'}, status=500)
    
    # Background Tasks
    
    async def peer_discovery_loop(self):
        """Background peer discovery"""
        while True:
            try:
                if len(self.peers) < self.max_peers:
                    # Try to discover new peers
                    await self.discover_new_peers()
                
                await asyncio.sleep(self.discovery_interval)
                
            except Exception as e:
                logger.error(f"Peer discovery loop error: {e}")
                await asyncio.sleep(60)
    
    async def peer_maintenance_loop(self):
        """Background peer maintenance"""
        while True:
            try:
                # Check peer health
                await self.check_peer_health()
                
                # Clean up disconnected peers
                await self.cleanup_disconnected_peers()
                
                await asyncio.sleep(60)  # Run every minute
                
            except Exception as e:
                logger.error(f"Peer maintenance error: {e}")
                await asyncio.sleep(60)
    
    async def data_sharing_loop(self):
        """Background data sharing and announcements"""
        while True:
            try:
                # Announce our data availability
                await self.announce_data_availability()
                
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except Exception as e:
                logger.error(f"Data sharing loop error: {e}")
                await asyncio.sleep(60)
    
    async def discover_new_peers(self):
        """Discover new peers through various methods"""
        # Peer exchange with existing peers
        for peer in list(self.peers.values())[:5]:  # Ask first 5 peers
            if peer.status == PeerStatus.CONNECTED:
                await self.exchange_peer_lists(peer)
        
        # Periodic local network scan
        if len(self.peers) < 10:  # Only scan if we have few peers
            local_peers = await self.scan_local_network()
            for address, port in local_peers[:3]:  # Limit connections
                if len(self.peers) < self.max_peers:
                    asyncio.create_task(self.connect_to_peer(address, port))
    
    async def check_peer_health(self):
        """Check health of connected peers"""
        for peer in list(self.peers.values()):
            if peer.status == PeerStatus.CONNECTED:
                if await self.ping_peer(peer):
                    peer.last_seen = datetime.now()
                else:
                    peer.status = PeerStatus.DISCONNECTED
                    logger.warning(f"Peer {peer.node_id} appears disconnected")
    
    async def ping_peer(self, peer: PeerNode) -> bool:
        """Ping a specific peer"""
        try:
            if peer.node_id not in self.peer_sessions:
                return False
            
            session = self.peer_sessions[peer.node_id]
            url = f"http://{peer.address}:{peer.port}/peer/ping"
            
            async with session.get(url) as response:
                return response.status == 200
                
        except:
            return False
    
    async def cleanup_disconnected_peers(self):
        """Remove old disconnected peers"""
        cutoff_time = datetime.now() - timedelta(hours=1)
        
        to_remove = []
        for peer_id, peer in self.peers.items():
            if (peer.status == PeerStatus.DISCONNECTED and 
                peer.last_seen < cutoff_time):
                to_remove.append(peer_id)
        
        for peer_id in to_remove:
            # Close session
            if peer_id in self.peer_sessions:
                await self.peer_sessions[peer_id].close()
                del self.peer_sessions[peer_id]
            
            # Remove peer
            del self.peers[peer_id]
            self.stats['peer_disconnections'] += 1
            logger.info(f"Removed disconnected peer {peer_id}")
    
    async def announce_data_availability(self):
        """Announce our data availability to peers"""
        if not self.storage:
            return
        
        # Get summary of our data
        stats = self.storage.get_storage_stats()
        
        announcement = {
            'node_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'cache_entries': stats['storage_stats']['total_entries'],
            'data_types': stats['storage_stats']['data_type_breakdown'],
            'storage_capacity': stats['storage_stats']['max_storage_mb']
        }
        
        # Send to connected peers
        for peer in self.peers.values():
            if peer.status == PeerStatus.CONNECTED:
                asyncio.create_task(self.send_announcement(peer, announcement))
    
    async def send_announcement(self, peer: PeerNode, announcement: Dict[str, Any]):
        """Send data availability announcement to a peer"""
        try:
            if peer.node_id not in self.peer_sessions:
                return
            
            session = self.peer_sessions[peer.node_id]
            url = f"http://{peer.address}:{peer.port}/network/announce"
            
            async with session.post(url, json=announcement) as response:
                if response.status == 200:
                    logger.debug(f"Announcement sent to {peer.node_id}")
                    
        except Exception as e:
            logger.debug(f"Failed to send announcement to {peer.node_id}: {e}")
    
    def get_network_status(self) -> Dict[str, Any]:
        """Get comprehensive network status"""
        connected_peers = [p for p in self.peers.values() if p.status == PeerStatus.CONNECTED]
        
        return {
            'node_id': self.node_id,
            'listen_port': self.listen_port,
            'peer_counts': {
                'total_discovered': len(self.peers),
                'connected': len(connected_peers),
                'max_peers': self.max_peers
            },
            'network_health': {
                'average_latency': sum(p.latency_ms for p in connected_peers) / max(len(connected_peers), 1),
                'average_reliability': sum(p.reliability_score for p in connected_peers) / max(len(connected_peers), 1),
                'total_data_shared': sum(p.data_shared for p in connected_peers),
                'total_data_received': sum(p.data_received for p in connected_peers)
            },
            'statistics': self.stats,
            'top_peers': [
                {
                    'node_id': p.node_id,
                    'address': p.full_address,
                    'reliability': p.reliability_score,
                    'latency_ms': p.latency_ms,
                    'data_shared': p.data_shared,
                    'storage_usage': p.storage_usage_percent
                }
                for p in sorted(connected_peers, key=lambda x: x.reliability_score, reverse=True)[:10]
            ]
        }