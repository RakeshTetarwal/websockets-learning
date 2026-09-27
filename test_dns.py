import asyncio
import socket


async def resolve_demo():
    loop = asyncio.get_running_loop()
    print("🔍 Asynchronously resolving google.com...")

    # This is the exact method asyncio calls inside loop.create_connection:
    addr_infos = await loop.getaddrinfo("google.com", 443, type=socket.SOCK_STREAM)

    print(f"Found {len(addr_infos)} address entries:\n")
    for info in addr_infos:
        family, socktype, proto, canonname, sockaddr = info
        fam_str = "IPv4 (AF_INET)" if family == socket.AF_INET else "IPv6 (AF_INET6)"
        ip = sockaddr[0]
        port = sockaddr[1]
        print(f" • Family: {fam_str:<16} IP: {ip:<32} Port: {port}")


if __name__ == "__main__":
    asyncio.run(resolve_demo())
