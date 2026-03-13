import React from 'react';
import { Check } from 'lucide-react';

const plans = [
  {
    name: 'Starter',
    price: '$49',
    period: '/mo',
    description: 'Perfect for small brokerages getting started.',
    features: ['1,000 Records / Day', 'Manual Downloads', 'Basic Carrier Info', 'Email Support'],
    cta: 'Get Started',
    popular: false,
  },
  {
    name: 'Pro',
    price: '$149',
    period: '/mo',
    description: 'For growing teams needing serious data.',
    features: ['50,000 Records / Day', 'Auto-Sync to CRM', 'Advanced Filtering', 'Priority Support', 'API Access'],
    cta: 'Upgrade to Pro',
    popular: true,
  },
  {
    name: 'Enterprise',
    price: '$499',
    period: '/mo',
    description: 'Unlimited scale for large logistics firms.',
    features: ['Unlimited Records', 'Dedicated Account Manager', 'Custom Integration', 'SLA Guarantee', 'White-label Reports'],
    cta: 'Contact Sales',
    popular: false,
  }
];

export const Subscription: React.FC = () => {
  return (
    <div className="p-8 pb-20 overflow-y-auto h-screen">
      <div className="text-center max-w-2xl mx-auto mb-16">
        <h2 className="text-indigo-400 font-semibold tracking-wide uppercase text-sm mb-3">Pricing Plans</h2>
        <h1 className="text-4xl font-bold text-white mb-6">Choose your data power</h1>
        <p className="text-lg text-slate-400">
          Unlock the full potential of the FMCSA database with our AI-powered extraction engine.
          Stop manual copy-pasting and start closing deals.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-6xl mx-auto">
        {plans.map((plan, idx) => (
          <div 
            key={idx} 
            className={`relative p-8 rounded-3xl border ${
              plan.popular 
                ? 'bg-slate-800/80 border-indigo-500 shadow-2xl shadow-indigo-500/10 scale-105 z-10' 
                : 'bg-slate-900/50 border-slate-800 hover:border-slate-700'
            } flex flex-col transition-all duration-300`}
          >
            {plan.popular && (
              <div className="absolute -top-4 left-1/2 -translate-x-1/2 bg-gradient-to-r from-indigo-500 to-purple-600 text-white px-4 py-1 rounded-full text-sm font-bold shadow-lg">
                Most Popular
              </div>
            )}
            
            <div className="mb-8">
              <h3 className="text-xl font-bold text-white mb-2">{plan.name}</h3>
              <p className="text-slate-400 text-sm h-10">{plan.description}</p>
            </div>

            <div className="mb-8">
              <span className="text-5xl font-bold text-white">{plan.price}</span>
              <span className="text-slate-500">{plan.period}</span>
            </div>

            <button 
              className={`w-full py-4 rounded-xl font-bold mb-8 transition-all ${
                plan.popular
                  ? 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-lg shadow-indigo-500/25'
                  : 'bg-slate-800 hover:bg-slate-700 text-white'
              }`}
            >
              {plan.cta}
            </button>

            <div className="space-y-4 flex-1">
              {plan.features.map((feature, fIdx) => (
                <div key={fIdx} className="flex items-center gap-3">
                  <div className={`rounded-full p-1 ${plan.popular ? 'bg-indigo-500/20 text-indigo-400' : 'bg-slate-800 text-slate-400'}`}>
                    <Check size={14} />
                  </div>
                  <span className="text-slate-300 text-sm">{feature}</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      
      <div className="mt-20 text-center border-t border-slate-800 pt-10">
        <p className="text-slate-500 text-sm">
          Secure payment processing via Stripe. Cancel anytime. 
          <br />Need a custom data solution? <a href="#" className="text-indigo-400 hover:underline">Chat with us.</a>
        </p>
      </div>
    </div>
  );
};