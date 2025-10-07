import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Arbitrage Scanner UI',
  description:
    'Визуализация крипто-спредов с серверного API arbitrage-scanner.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ru">
      <body>
        <main>{children}</main>
      </body>
    </html>
  );
}
