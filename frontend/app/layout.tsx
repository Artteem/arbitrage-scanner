import type { Metadata } from 'next';
import Image from 'next/image';
import Link from 'next/link';
import './globals.css';

export const metadata: Metadata = {
  title: 'AY-Trade',
  description: 'Визуализация крипто-спредов с серверного API arbitrage-scanner.',
  icons: {
    icon: '/favicon.png',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ru">
      <body>
        <header className="site-header">
          <div className="site-header-inner">
            <Link href="/" className="site-logo">
              <Image
                className="site-logo-image site-logo-image-light"
                src="/branding/aytrade-header-light.png"
                alt=""
                priority
                width={512}
                height={102}
                sizes="(max-width: 600px) 240px, 360px"
                style={{ width: '100%', height: 'auto' }}
              />
              <Image
                className="site-logo-image site-logo-image-dark"
                src="/branding/aytrade-header-dark.png"
                alt=""
                aria-hidden="true"
                priority
                width={512}
                height={102}
                sizes="(max-width: 600px) 240px, 360px"
                style={{ width: '100%', height: 'auto' }}
              />
              <span className="sr-only">AY-Trade — на главную</span>
            </Link>
          </div>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
