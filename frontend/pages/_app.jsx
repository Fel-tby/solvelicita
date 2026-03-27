import '../styles/globals.css'
import 'leaflet/dist/leaflet.css'
import { Analytics } from '@vercel/analytics/next'

export default function App({ Component, pageProps }) {
  return (
    <>
      <Component {...pageProps} />
      <Analytics />
    </>
  )
}
