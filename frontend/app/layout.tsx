import type { Metadata } from 'next';
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
              <span className="site-logo-image" aria-hidden="true" />
              <span className="sr-only">AY-Trade — на главную</span>
            </Link>
          </div>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
